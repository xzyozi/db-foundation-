"""Issue #7 性能検証ベンチマーク（1パーティション数十万件の書き込みと全件再生）。

手動実行専用（通常の CI 必須判定には含めない）。
実行例:
    uv run --no-sync python benchmarks/bench_storage_kernel.py
    uv run --no-sync python benchmarks/bench_storage_kernel.py --runs 3 --counts 10000 100000 300000
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from typing import Any

from storage_kernel import KernelConfig, StorageKernel
from storage_kernel.contrib.sqlite import SqliteMetaDAO, SqliteShardDAO
from storage_kernel.testing.memory_ports import JsonDeterministicCodec, SimplePartitionRouter


@dataclass(frozen=True)
class WriteRunMetrics:
    case_name: str
    payload_mode: str
    payload_bytes: int
    run_index: int
    target_count: int
    partition_count: int
    elapsed_sec: float
    writes_per_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    db_size_bytes: int
    error_count: int
    seq_valid: bool


@dataclass(frozen=True)
class ReplayRunMetrics:
    case_name: str
    payload_mode: str
    run_index: int
    fetched_count: int
    elapsed_sec: float
    reads_per_sec: float
    peak_tracemalloc_mb: float
    max_rss_mb: float
    seq_valid: bool
    os_cache_note: str


def _make_payload(mode: str, index: int) -> dict[str, Any]:
    if mode == "typical":
        return {"index": index, "kind": "order_tick", "body": "x" * 440}
    if mode == "large":
        return {"index": index, "kind": "large_blob", "body": "x" * 60_000}
    raise ValueError(f"unsupported payload mode: {mode}")


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    pos = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def _total_db_size(base_dir: Path) -> int:
    total = 0
    for p in base_dir.rglob("*"):
        if p.is_file():
            total += p.stat().st_size
    return total


def _max_rss_mb() -> float:
    """プロセスの最大常駐メモリ（MiB）。OS ごとに取得方法が異なるため、プラットフォームで分岐する。"""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            raise ctypes.WinError(ctypes.get_last_error())
        return counters.PeakWorkingSetSize / (1024.0 * 1024.0)

    import resource  # POSIX のみ（Windows には存在しない）

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS returns bytes; Linux returns kilobytes
    if sys.platform == "darwin":
        return rss / (1024.0 * 1024.0)
    return rss / 1024.0


def _build_kernel(base_dir: Path) -> StorageKernel[dict[str, Any]]:
    codec = JsonDeterministicCodec()
    meta_dao = SqliteMetaDAO(base_dir / "meta.db", durability="power_loss")
    shard_dao = SqliteShardDAO[bytes](base_dir / "shards", codec=None, durability="power_loss")
    return StorageKernel(
        config=KernelConfig(schema_registry={"bench": 1}),
        shard_dao=shard_dao,
        meta_dao=meta_dao,
        codec=codec,
        router=SimplePartitionRouter(default_shards=4),
        store_raw_bytes=True,
    )


def _verify_replay(
    kernel: StorageKernel[dict[str, Any]],
    partition_key: str,
    expected_count: int,
    *,
    limit_per_partition: int = 1000,
) -> tuple[int, bool, float, float]:
    tracemalloc.start()
    t0 = time.perf_counter()
    cursor = 0
    fetched = 0
    seq_valid = True
    expected_seq = 1

    while True:
        batch = kernel.sync({partition_key: cursor}, limit_per_partition=limit_per_partition)
        records = batch.get(partition_key, [])
        if not records:
            break
        for rec in records:
            if rec.seq != expected_seq:
                seq_valid = False
            expected_seq += 1
        fetched += len(records)
        cursor = records[-1].seq

    elapsed = time.perf_counter() - t0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    if fetched != expected_count:
        seq_valid = False
    return fetched, seq_valid, elapsed, peak_bytes / (1024.0 * 1024.0)


def _verify_multi_partition_seqs(
    kernel: StorageKernel[dict[str, Any]],
    partition_count: int,
    expected_total: int,
) -> bool:
    cursors = {f"benchmark:multi:{i:03d}": 0 for i in range(partition_count)}
    expected_next = {pk: 1 for pk in cursors}
    total_fetched = 0

    while True:
        batch = kernel.sync(cursors, limit_per_partition=1000)
        if not batch:
            break
        for pk, records in batch.items():
            for rec in records:
                if rec.seq != expected_next[pk]:
                    return False
                expected_next[pk] += 1
            total_fetched += len(records)
            cursors[pk] = records[-1].seq
    return total_fetched == expected_total


def run_single_partition_case(
    *,
    count: int,
    payload_mode: str,
    run_index: int,
    run_replay: bool,
) -> tuple[WriteRunMetrics, ReplayRunMetrics | None, ReplayRunMetrics | None]:
    codec = JsonDeterministicCodec()
    payload_size = len(codec.encode(_make_payload(payload_mode, 0)))

    with tempfile.TemporaryDirectory(prefix="sk_bench_single_") as tmp:
        base_dir = Path(tmp)
        kernel = _build_kernel(base_dir)
        latencies_ms: list[float] = []
        errors = 0

        t_start = time.perf_counter()
        for idx in range(count):
            t_op = time.perf_counter_ns()
            try:
                kernel.append(
                    "benchmark:single",
                    "bench",
                    _make_payload(payload_mode, idx),
                    f"single-{idx}",
                    schema_version=1,
                )
            except Exception:
                errors += 1
            latencies_ms.append((time.perf_counter_ns() - t_op) / 1_000_000.0)
        total_elapsed = time.perf_counter() - t_start

        latencies_ms.sort()
        db_size = _total_db_size(base_dir)

        replay_same_proc: ReplayRunMetrics | None = None
        replay_restarted: ReplayRunMetrics | None = None

        if run_replay:
            fetched, seq_valid, r_elapsed, peak_mb = _verify_replay(kernel, "benchmark:single", count)
            replay_same_proc = ReplayRunMetrics(
                case_name=f"replay_in_process_{count}",
                payload_mode=payload_mode,
                run_index=run_index,
                fetched_count=fetched,
                elapsed_sec=r_elapsed,
                reads_per_sec=fetched / r_elapsed if r_elapsed > 0 else 0.0,
                peak_tracemalloc_mb=peak_mb,
                max_rss_mb=_max_rss_mb(),
                seq_valid=seq_valid,
                os_cache_note="same_process_warm_os_cache",
            )
            kernel.close()

            proc = subprocess.run(
                [
                    sys.executable,
                    __file__,
                    "--replay-subprocess",
                    str(base_dir),
                    "benchmark:single",
                    str(count),
                    payload_mode,
                    str(run_index),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            sub_data = json.loads(proc.stdout.strip())
            replay_restarted = ReplayRunMetrics(**sub_data)
            seq_ok = replay_same_proc.seq_valid and replay_restarted.seq_valid
        else:
            fetched, seq_ok, _, _ = _verify_replay(kernel, "benchmark:single", count)
            kernel.close()

        write_metrics = WriteRunMetrics(
            case_name=f"single_partition_{count}",
            payload_mode=payload_mode,
            payload_bytes=payload_size,
            run_index=run_index,
            target_count=count,
            partition_count=1,
            elapsed_sec=total_elapsed,
            writes_per_sec=count / total_elapsed if total_elapsed > 0 else 0.0,
            p50_ms=_percentile(latencies_ms, 50.0),
            p95_ms=_percentile(latencies_ms, 95.0),
            p99_ms=_percentile(latencies_ms, 99.0),
            db_size_bytes=db_size,
            error_count=errors,
            seq_valid=seq_ok,
        )
        return write_metrics, replay_same_proc, replay_restarted


def run_multi_partition_case(
    *,
    partition_count: int,
    total_count: int,
    payload_mode: str,
    run_index: int,
) -> WriteRunMetrics:
    codec = JsonDeterministicCodec()
    payload_size = len(codec.encode(_make_payload(payload_mode, 0)))

    with tempfile.TemporaryDirectory(prefix="sk_bench_multi_") as tmp:
        base_dir = Path(tmp)
        kernel = _build_kernel(base_dir)
        latencies_ms: list[float] = []
        errors = 0

        t_start = time.perf_counter()
        for idx in range(total_count):
            pk = f"benchmark:multi:{idx % partition_count:03d}"
            t_op = time.perf_counter_ns()
            try:
                kernel.append(
                    pk,
                    "bench",
                    _make_payload(payload_mode, idx),
                    f"multi-{idx}",
                    schema_version=1,
                )
            except Exception:
                errors += 1
            latencies_ms.append((time.perf_counter_ns() - t_op) / 1_000_000.0)
        total_elapsed = time.perf_counter() - t_start

        latencies_ms.sort()
        db_size = _total_db_size(base_dir)
        seq_ok = _verify_multi_partition_seqs(kernel, partition_count, total_count)
        kernel.close()

        return WriteRunMetrics(
            case_name=f"multi_partition_{partition_count}p_{total_count}",
            payload_mode=payload_mode,
            payload_bytes=payload_size,
            run_index=run_index,
            target_count=total_count,
            partition_count=partition_count,
            elapsed_sec=total_elapsed,
            writes_per_sec=total_count / total_elapsed if total_elapsed > 0 else 0.0,
            p50_ms=_percentile(latencies_ms, 50.0),
            p95_ms=_percentile(latencies_ms, 95.0),
            p99_ms=_percentile(latencies_ms, 99.0),
            db_size_bytes=db_size,
            error_count=errors,
            seq_valid=seq_ok,
        )


def _replay_subprocess_entry(args: list[str]) -> None:
    base_dir = Path(args[0])
    partition_key = args[1]
    expected_count = int(args[2])
    payload_mode = args[3]
    run_index = int(args[4])

    kernel = _build_kernel(base_dir)
    try:
        fetched, seq_valid, elapsed, peak_mb = _verify_replay(kernel, partition_key, expected_count)
        metrics = ReplayRunMetrics(
            case_name=f"replay_restarted_process_{expected_count}",
            payload_mode=payload_mode,
            run_index=run_index,
            fetched_count=fetched,
            elapsed_sec=elapsed,
            reads_per_sec=fetched / elapsed if elapsed > 0 else 0.0,
            peak_tracemalloc_mb=peak_mb,
            max_rss_mb=_max_rss_mb(),
            seq_valid=seq_valid,
            os_cache_note="new_process_warm_os_page_cache",
        )
        print(json.dumps(asdict(metrics)))
    finally:
        kernel.close()


def _format_markdown_report(
    write_results: list[WriteRunMetrics],
    replay_results: list[ReplayRunMetrics],
) -> str:
    lines: list[str] = [
        "# Issue #7 性能検証結果",
        "",
        f"- **Python**: {platform.python_version()}",
        f"- **OS / Arch**: {platform.platform()}",
        f"- **CPU cores**: {os.cpu_count()}",
        "- **Durability**: `power_loss` (`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;`)",
        "",
        "## 書き込みベンチマーク結果（全試行の中央値 / 範囲）",
        "",
        "| ケース | payload | サイズ | 試行回数 | writes/sec (中央値) | p50 (ms) | p95 (ms) | p99 (ms) "
        "| 総時間 (s) | DBサイズ (MiB) | エラー | seq整合 |",
        "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |",
    ]

    write_groups: dict[tuple[str, str], list[WriteRunMetrics]] = {}
    for w in write_results:
        write_groups.setdefault((w.case_name, w.payload_mode), []).append(w)

    for (case_name, mode), items in write_groups.items():
        wps = statistics.median(i.writes_per_sec for i in items)
        p50 = statistics.median(i.p50_ms for i in items)
        p95 = statistics.median(i.p95_ms for i in items)
        p99 = statistics.median(i.p99_ms for i in items)
        elapsed = statistics.median(i.elapsed_sec for i in items)
        db_mib = statistics.median(i.db_size_bytes for i in items) / (1024.0 * 1024.0)
        errs = sum(i.error_count for i in items)
        seq_ok = "OK" if all(i.seq_valid for i in items) else "NG"
        lines.append(
            f"| {case_name} | {mode} | {items[0].payload_bytes} B | {len(items)} | "
            f"{wps:.1f} | {p50:.3f} | {p95:.3f} | {p99:.3f} | {elapsed:.2f} | {db_mib:.2f} | {errs} | {seq_ok} |"
        )

    lines.extend(
        [
            "",
            "## 全件再生ベンチマーク結果（カーソル 0 からの `sync()`）",
            "",
            "| ケース | payload | 試行回数 | 再生時間 (s) | reads/sec (中央値) "
            "| peak tracemalloc (MiB) | max RSS (MiB) | seq整合 | OSキャッシュ注記 |",
            "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | :---: | :--- |",
        ]
    )

    replay_groups: dict[tuple[str, str], list[ReplayRunMetrics]] = {}
    for r in replay_results:
        replay_groups.setdefault((r.case_name, r.payload_mode), []).append(r)

    for (case_name, mode), r_items in replay_groups.items():
        elapsed = statistics.median(i.elapsed_sec for i in r_items)
        rps = statistics.median(i.reads_per_sec for i in r_items)
        peak = statistics.median(i.peak_tracemalloc_mb for i in r_items)
        rss = statistics.median(i.max_rss_mb for i in r_items)
        seq_ok = "OK" if all(i.seq_valid for i in r_items) else "NG"
        lines.append(
            f"| {case_name} | {mode} | {len(r_items)} | {elapsed:.3f} | {rps:.1f} | "
            f"{peak:.2f} | {rss:.2f} | {seq_ok} | {r_items[0].os_cache_note} |"
        )

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] == "--replay-subprocess":
        _replay_subprocess_entry(raw_args[1:])
        return 0

    parser = argparse.ArgumentParser(description="storage_kernel Issue #7 性能ベンチマーク")
    parser.add_argument("--runs", type=int, default=3, help="各試験の試行回数（既定: 3）")
    parser.add_argument(
        "--counts",
        type=int,
        nargs="+",
        default=[10_000, 100_000, 300_000],
        help="単一パーティション試験の件数リスト（既定: 10000 100000 300000）",
    )
    parser.add_argument("--multi-partitions", type=int, default=100, help="分散試験のパーティション数")
    parser.add_argument("--multi-count", type=int, default=300_000, help="分散試験の総件数")
    parser.add_argument(
        "--payload-modes",
        nargs="+",
        choices=["typical", "large"],
        default=["typical", "large"],
        help="計測するペイロード種別",
    )
    parser.add_argument("--output-json", type=Path, default=None, help="生計測値の JSON 出力先")
    parser.add_argument("--output-md", type=Path, default=None, help="集計結果の Markdown 出力先")
    args = parser.parse_args(raw_args)

    write_results: list[WriteRunMetrics] = []
    replay_results: list[ReplayRunMetrics] = []
    max_single_count = max(args.counts)

    for mode in args.payload_modes:
        for count in args.counts:
            do_replay = count == max_single_count
            for run_idx in range(1, args.runs + 1):
                print(
                    f"[bench] single count={count} mode={mode} run={run_idx}/{args.runs}",
                    file=sys.stderr,
                    flush=True,
                )
                w, r_same, r_sub = run_single_partition_case(
                    count=count,
                    payload_mode=mode,
                    run_index=run_idx,
                    run_replay=do_replay,
                )
                write_results.append(w)
                if r_same is not None:
                    replay_results.append(r_same)
                if r_sub is not None:
                    replay_results.append(r_sub)

        for run_idx in range(1, args.runs + 1):
            print(
                f"[bench] multi partitions={args.multi_partitions} total={args.multi_count} "
                f"mode={mode} run={run_idx}/{args.runs}",
                file=sys.stderr,
                flush=True,
            )
            w_multi = run_multi_partition_case(
                partition_count=args.multi_partitions,
                total_count=args.multi_count,
                payload_mode=mode,
                run_index=run_idx,
            )
            write_results.append(w_multi)

    report = _format_markdown_report(write_results, replay_results)
    print(report)

    if args.output_json is not None:
        payload = {
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "durability": "power_loss",
            },
            "writes": [asdict(w) for w in write_results],
            "replays": [asdict(r) for r in replay_results],
        }
        args.output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    if args.output_md is not None:
        args.output_md.write_text(report, encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
