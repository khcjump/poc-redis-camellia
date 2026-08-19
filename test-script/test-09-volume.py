#!/usr/bin/env python3
"""
test-09-volume.py ─ Redis 極限 TPS 壓力測試
=============================================

目標：以本機最大資源（多 Process × 多 Thread × Pipeline）
      對 onprem-redis-master 或 cloud-redis-write 進行壓測，量測最大 TPS。

使用方式：
    # 測試 onprem-master（預設）
    python3 test-script/test-09-volume.py

    # 測試 cloud-write
    python3 test-script/test-09-volume.py --target cloud

    # 自訂並行度
    python3 test-script/test-09-volume.py --target onprem --workers 8 --threads 16 --duration 30

目標 Redis（對應 docker-compose.yml）：
    onprem-master : 127.0.0.1:6403
    cloud-write   : 127.0.0.1:6401
    cloud-proxy   : 127.0.0.1:6380  (Camellia Proxy, 可用 --target cloud-proxy)
    onprem-proxy  : 127.0.0.1:6381  (Camellia Proxy, 可用 --target onprem-proxy)

測試項目：
    1. SET  ─ 純 KV 寫入
    2. HSET ─ Hash 欄位寫入
    3. ZADD ─ Sorted Set 寫入

架構：
    main
    ├── Worker Process × N (multiprocessing)
    │   └── Thread × M (threading)
    │       └── pipeline.execute() batch × PIPELINE_BATCH
    └── 最終彙整各 process 計數 → 計算 TPS
"""

import argparse
import multiprocessing
import threading
import time
import sys
import os
from dataclasses import dataclass
from typing import Dict

import redis

# ─────────────────────────────────────────────────────────────────────────────
# 目標清單（對應 docker-compose.yml port mapping）
# ─────────────────────────────────────────────────────────────────────────────
TARGETS: Dict[str, tuple] = {
    "onprem":       ("127.0.0.1", 6403),   # onprem-redis-master（直連）
    "cloud":        ("127.0.0.1", 6401),   # cloud-redis-write（直連）
    "cloud-proxy":  ("127.0.0.1", 6380),   # cloud-app Camellia Proxy
    "onprem-proxy": ("127.0.0.1", 6381),   # onprem-app Camellia Proxy
}

PIPELINE_BATCH = 200   # 每次 pipeline.execute() 打幾筆
KEY_PREFIX     = "vol" # key 前綴，測試後可 redis-cli SCAN + DEL 清理

# ─────────────────────────────────────────────────────────────────────────────
# 測試命令定義
# ─────────────────────────────────────────────────────────────────────────────
COMMANDS = {
    "SET":  lambda pipe, idx: pipe.set(f"{KEY_PREFIX}:str:{idx % 100_000}", f"v{idx}"),
    "HSET": lambda pipe, idx: pipe.hset(f"{KEY_PREFIX}:hash:{idx % 10_000}", f"f{idx % 100}", f"v{idx}"),
    "ZADD": lambda pipe, idx: pipe.zadd(f"{KEY_PREFIX}:zset:{idx % 10_000}", {f"m{idx % 1000}": idx}),
}


# ─────────────────────────────────────────────────────────────────────────────
# Worker 執行緒：對單一 Redis 連線做 pipeline 批量寫入
# ─────────────────────────────────────────────────────────────────────────────
def thread_worker(host: str, port: int, cmd_name: str,
                  stop_event: threading.Event, counter_lock: threading.Lock,
                  shared_counter: list, worker_id: int):
    """
    每個執行緒維護一條獨立的 Redis 連線（non-blocking_pool=False，直連），
    用 pipeline 批量送出指令以最大化吞吐量。
    """
    cmd_fn = COMMANDS[cmd_name]
    try:
        r = redis.Redis(host=host, port=port, socket_connect_timeout=3,
                        socket_timeout=5, decode_responses=False)
        r.ping()
    except Exception as e:
        print(f"  [Thread-{worker_id}] 連線失敗: {e}", flush=True)
        return

    idx = worker_id * 10_000_000  # 各 thread 使用不同 key 空間，避免競爭
    local_count = 0

    while not stop_event.is_set():
        try:
            pipe = r.pipeline(transaction=False)
            for _ in range(PIPELINE_BATCH):
                cmd_fn(pipe, idx)
                idx += 1
            pipe.execute()
            local_count += PIPELINE_BATCH
        except Exception:
            # 短暫網路抖動不終止，繼續下一批
            pass

    with counter_lock:
        shared_counter[0] += local_count


# ─────────────────────────────────────────────────────────────────────────────
# Worker Process：啟動多個 Thread，收集完成計數後回寫 Queue
# ─────────────────────────────────────────────────────────────────────────────
def process_worker(host: str, port: int, cmd_name: str,
                   threads_per_proc: int, duration_sec: int,
                   result_queue: multiprocessing.Queue, proc_id: int):
    """
    每個 Process 啟動 threads_per_proc 條執行緒，
    跑滿 duration_sec 秒後停止並回傳總計數。
    """
    stop_event = threading.Event()
    counter_lock = threading.Lock()
    shared_counter = [0]

    threads = []
    for t_id in range(threads_per_proc):
        wid = proc_id * threads_per_proc + t_id
        t = threading.Thread(
            target=thread_worker,
            args=(host, port, cmd_name, stop_event, counter_lock,
                  shared_counter, wid),
            daemon=True
        )
        threads.append(t)

    start = time.monotonic()
    for t in threads:
        t.start()

    time.sleep(duration_sec)
    stop_event.set()

    for t in threads:
        t.join(timeout=5)

    elapsed = time.monotonic() - start
    result_queue.put((proc_id, shared_counter[0], elapsed))


# ─────────────────────────────────────────────────────────────────────────────
# 執行單一命令的完整壓測，回傳 TPS
# ─────────────────────────────────────────────────────────────────────────────
def run_benchmark(host: str, port: int, cmd_name: str,
                  num_workers: int, threads_per_proc: int,
                  duration_sec: int) -> float:
    """
    啟動 num_workers 個 Process，每個 Process 跑 threads_per_proc 條 Thread，
    持續 duration_sec 秒，回傳量測到的 TPS。
    """
    result_queue = multiprocessing.Queue()
    procs = []

    t_start = time.time()
    for p_id in range(num_workers):
        p = multiprocessing.Process(
            target=process_worker,
            args=(host, port, cmd_name, threads_per_proc,
                  duration_sec, result_queue, p_id),
            daemon=True
        )
        procs.append(p)
        p.start()

    for p in procs:
        p.join(timeout=duration_sec + 10)

    total_ops = 0
    max_elapsed = 0.0
    while not result_queue.empty():
        _pid, ops, elapsed = result_queue.get_nowait()
        total_ops += ops
        max_elapsed = max(max_elapsed, elapsed)

    tps = total_ops / max_elapsed if max_elapsed > 0 else 0
    return tps, total_ops, max_elapsed


# ─────────────────────────────────────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────────────────────────────────────
def main():
    cpu_count = multiprocessing.cpu_count()

    parser = argparse.ArgumentParser(
        description="Redis 極限 TPS 壓力測試（multiprocessing + threading + pipeline）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--target", choices=list(TARGETS.keys()), default="onprem",
        help="測試目標 Redis"
    )
    parser.add_argument(
        "--workers", type=int, default=cpu_count,
        help="Worker Process 數（預設 = 本機 CPU 核心數）"
    )
    parser.add_argument(
        "--threads", type=int, default=8,
        help="每個 Process 的 Thread 數"
    )
    parser.add_argument(
        "--duration", type=int, default=20,
        help="每個命令的測試時長（秒）"
    )
    parser.add_argument(
        "--commands", nargs="+", choices=list(COMMANDS.keys()),
        default=list(COMMANDS.keys()),
        help="要測試的命令清單"
    )
    parser.add_argument(
        "--warmup", type=int, default=3,
        help="暖機時長（秒），預先建立連線與 JIT 穩定"
    )
    args = parser.parse_args()

    host, port = TARGETS[args.target]
    total_conns = args.workers * args.threads

    print("=" * 65)
    print("  Redis 極限 TPS 壓力測試")
    print("=" * 65)
    print(f"  目標        : {args.target}  ({host}:{port})")
    print(f"  CPU 核心數  : {cpu_count}")
    print(f"  Worker Procs: {args.workers}")
    print(f"  Threads/Proc: {args.threads}")
    print(f"  總連線數    : {total_conns}")
    print(f"  Pipeline批量: {PIPELINE_BATCH} ops/execute")
    print(f"  測試時長    : {args.duration}s / 命令")
    print(f"  暖機時長    : {args.warmup}s")
    print(f"  測試命令    : {', '.join(args.commands)}")
    print("=" * 65)

    # 連線前檢查
    try:
        r = redis.Redis(host=host, port=port, socket_connect_timeout=3)
        info_server = r.info("server")
        info_memory = r.info("memory")
        info_clients = r.info("clients")
        print(f"\n✅ 連線成功！Redis {info_server.get('redis_version', '?')} @ {host}:{port}")
        print(f"   used_memory_human : {info_memory.get('used_memory_human', '?')}")
        print(f"   connected_clients : {info_clients.get('connected_clients', '?')}")
        r.close()
    except Exception as e:
        print(f"\n❌ 無法連線到 {host}:{port} — {e}")
        print("   請確認 docker-compose 環境已啟動")
        sys.exit(1)

    # 暖機（避免首次 TCP 建立、OS cache miss 影響數據）
    if args.warmup > 0:
        print(f"\n⏳ 暖機 {args.warmup}s（不計入結果）...", end="", flush=True)
        run_benchmark(host, port, "SET", args.workers, args.threads, args.warmup)
        print(" done")

    # 各命令壓測
    results = {}
    print()
    for cmd in args.commands:
        print(f"🔥 測試 {cmd} ...", end="", flush=True)
        tps, total_ops, elapsed = run_benchmark(
            host, port, cmd, args.workers, args.threads, args.duration
        )
        results[cmd] = (tps, total_ops, elapsed)
        tps_k = tps / 1000
        print(f" {tps:>12,.0f} ops/s  ({tps_k:.1f}k TPS)  ─ {total_ops:,} ops in {elapsed:.1f}s")

    # 結果彙整
    print()
    print("=" * 65)
    print("  壓測結果彙整")
    print("=" * 65)
    print(f"  {'命令':<8} {'TPS':>14} {'千TPS':>8} {'總操作數':>14} {'時長(s)':>8}")
    print(f"  {'-'*8} {'-'*14} {'-'*8} {'-'*14} {'-'*8}")
    for cmd, (tps, total_ops, elapsed) in results.items():
        print(f"  {cmd:<8} {tps:>14,.0f} {tps/1000:>7.1f}k {total_ops:>14,} {elapsed:>7.1f}s")
    print("=" * 65)

    best_cmd = max(results, key=lambda c: results[c][0])
    best_tps = results[best_cmd][0]
    print(f"\n🏆 最高 TPS：{best_cmd}  {best_tps:,.0f} ops/s  ({best_tps/1000:.1f}k)")
    print(f"   Workers={args.workers}  Threads/Proc={args.threads}  Pipeline={PIPELINE_BATCH}")
    print(f"   理論單連線 TPS = {best_tps / total_conns:,.0f}")

    # 測試後查看 Redis info
    try:
        r = redis.Redis(host=host, port=port, socket_connect_timeout=3)
        info_memory = r.info("memory")
        info_clients = r.info("clients")
        info_stat   = r.info("stats")
        print(f"\n📊 測試後 Redis 狀態：")
        print(f"   used_memory_human     : {info_memory.get('used_memory_human', '?')}")
        print(f"   connected_clients     : {info_clients.get('connected_clients', '?')}")
        print(f"   total_commands_procs  : {info_stat.get('total_commands_processed', '?'):,}")
        print(f"   instantaneous_ops_sec : {info_stat.get('instantaneous_ops_per_sec', '?'):,}")
        r.close()
    except Exception:
        pass

    print()


if __name__ == "__main__":
    # macOS: fork 可能與 Objective-C runtime 衝突，用 spawn
    multiprocessing.set_start_method("spawn", force=True)
    main()
