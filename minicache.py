#!/usr/bin/env python3
"""
minicache.py -- 纯标准库实现的双协议多路复用内存缓存原型。

特性
----
* 分段锁存储引擎: N 个互不依赖的分片, 每个分片一把锁, 彻底消除全局大锁;
  任何操作路径最多只持有一把锁, 结构上不可能产生锁序环 => 无死锁。
* CLOCK(二次机会)近似 LRU: 命中只需一次 dict 取数 + 标志位置位(O(1)),
  淘汰以 FIFO 环扫描 + referenced 位降级实现, 高频读写下开销极低。
* TTL 过期: 每分片独立过期最小堆 + 惰性删除 + 后台 janitor 线程主动清扫。
* 内存/条数双容量上限, 超限当场淘汰, 杜绝超额穿透; 大键(超过分片配额)拒绝写入。
* 单端口多路复用: selectors + 非阻塞 socket 同时承载
   - 原始 TCP 行文本协议 (PING/GET/SET/DEL/EXPIRE/KEYS/STATS/FLUSH)
   - 极简 HTTP/1.1 GET  (/          仪表盘, /stats  JSON, /keys JSON)
  慢客户端的读写阻塞完全不会拖累事件循环 (背压时由 selector 监听可写)。
* 内置后台随机读写压测线程, 启动即可在浏览器观察原子计数指标。
* 内置引擎级并发一致性单测 + 真实端口网络自测:
      python minicache.py --test

运行
----
    python minicache.py                 # 默认 :9900, 16 分片, 64 MiB 上限
    python minicache.py --port 8080 --segments 32 --max-bytes 33554432
    python minicache.py --no-load       # 关闭内置压测
    python minicache.py --test          # 运行全部内置测试

TCP 协议示例 (Windows PowerShell)
    $c = [System.Net.Sockets.TcpClient]::new("127.0.0.1", 9900)
    $s = $c.GetStream(); $w = [IO.StreamWriter]::new($s)
    $w.WriteLine("SET foo 3600 hello world"); $w.Flush()
    (New-Object IO.StreamReader($s)).ReadLine()   # -> STORED

TCP SET 语法为定界 4 段:  SET <key> <ttl_seconds> <value...>
ttl=0 表示永不过期; 协议以行(\n)为帧, value 可包含空格。
"""

from __future__ import annotations

import argparse
import collections
import heapq
import html
import itertools
import json
import random
import re
import selectors
import socket
import threading
import time
import unittest
import urllib.request
import urllib.error
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# 常量与原子计数
# ---------------------------------------------------------------------------

VERSION = "minicache/1.0"
MAX_LINE_BYTES = 1 << 20          # 单条协议行硬上限 1 MiB
MAX_OUTPUT_BYTES = 8 << 20        # 单连接待发送缓冲上限 8 MiB, 防止慢客户端拖垮内存
HTTP_KEY_LIMIT = 500
TCP_KEY_LIMIT = 200
DEFAULT_PORT = 9900

# 过期堆的全局单调序号: 让相同到期时刻的堆元素比较不触碰 bytes, 同时保证唯一
_HEAP_SEQ = itertools.count()


class AtomicStats:
    """所有计数器由各自分片的锁保护; 快照时短时分别取锁再聚合。"""

    __slots__ = ("hits", "misses", "sets", "evictions", "expirations", "overflows")

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.sets = 0
        self.evictions = 0
        self.expirations = 0
        self.overflows = 0


# ---------------------------------------------------------------------------
# 存储引擎: Entry / Segment / Cache
# ---------------------------------------------------------------------------

_ENTRY_FIXED_COST = 128  # 单个 Entry 近似的 Python 对象固定开销(字节)


class Entry:
    __slots__ = ("value", "expire_at", "referenced")

    def __init__(self, value: bytes, expire_at: float | None) -> None:
        self.value = value
        self.expire_at = expire_at      # time.monotonic() 时刻; None 表示永久
        self.referenced = True          # CLOCK 二次机会位

    def cost(self, key_len: int) -> int:
        return _ENTRY_FIXED_COST + key_len + len(self.value)


@dataclass
class _Segment:
    index: int
    max_bytes: int
    max_entries: int

    def __post_init__(self) -> None:
        self.lock = threading.Lock()
        self.data: dict[bytes, Entry] = {}
        # 稳定的 FIFO 环形队列; CLOCK 游标 hand_idx 只在其上单调前进。
        # 与 dict 解耦: 覆盖键值不会改变其在环中的位置。
        self.ring: collections.deque[bytes] = collections.deque()
        self.hand_idx = 0
        # 过期堆元素: (expire_at, seq, key); seq 保证同刻时刻比较不触碰 bytes
        self.expiry: list[tuple[float, int, bytes]] = []
        self.used_bytes = 0
        self.stats = AtomicStats()

    # ----- 内部: 必须已持锁 ----------------------------------------------

    def _remove_locked(self, key: bytes) -> Entry | None:
        entry = self.data.pop(key, None)
        if entry is not None:
            self.used_bytes -= entry.cost(len(key))
            self._unlink_from_ring_locked(key)
        return entry

    def _unlink_from_ring_locked(self, key: bytes) -> None:
        """从环形队列删除任意键并修正游标。O(N) 但仅在逐出/显式删除/过期时
        发生, 热读路径完全不触碰环, 这正是 CLOCK 相对 LRU 的低开销优势。
        幂等: 键不在环中(如调用方先 pop)也安全。"""
        try:
            idx = self.ring.index(key)
        except ValueError:
            return
        del self.ring[idx]
        n = len(self.ring)
        if n == 0:
            self.hand_idx = 0
        elif idx < self.hand_idx:
            self.hand_idx -= 1
        elif self.hand_idx >= n:
            self.hand_idx = 0

    def _clock_next_locked(self) -> tuple[bytes, Entry] | None:
        """返回游标当前候选, 并把游标前移一位(先消费后推进)。"""
        if not self.ring:
            self.hand_idx = 0
            return None
        self.hand_idx %= len(self.ring)
        key = self.ring[self.hand_idx]
        self.hand_idx = (self.hand_idx + 1) % len(self.ring)
        return key, self.data[key]

    def _pop_expired_locked(self, now: float) -> bool:
        """堆顶到期则删除对应键并计过期, 返回是否删除了一个键。"""
        heap = self.expiry
        while heap and heap[0][0] <= now:
            _, _, key = heapq.heappop(heap)
            entry = self.data.get(key)
            if entry is None or entry.expire_at is None or entry.expire_at > now:
                continue  # 堆里残留的旧版本(已覆盖/已删), 丢弃即可
            self._remove_locked(key)
            self.stats.expirations += 1
            return True
        return False

    def _evict_one_locked(self, now: float) -> bool:
        """淘汰一个键: 优先过期堆顶, 否则走 CLOCK。
        hand 游标跨调用保持, 一次调用持续扫描(跨越多轮降级)直到真正逐出一个
        键为止; 至多扫描 2N 个候选 => 有界、无活锁。"""
        if self._pop_expired_locked(now):
            return True
        if not self.data:
            return False
        guard = len(self.data) * 2
        while guard:
            guard -= 1
            advanced = self._clock_next_locked()
            if advanced is None:
                return False
            key, entry = advanced
            if not entry.referenced:
                self._remove_locked(key)
                self.stats.evictions += 1
                return True
            entry.referenced = False         # 给过一次机会, 下轮淘汰
        return False

    # ----- 公共操作 ------------------------------------------------------

    def get(self, key: bytes, now: float) -> bytes | None:
        with self.lock:
            entry = self.data.get(key)
            if entry is None:
                self.stats.misses += 1
                return None
            if entry.expire_at is not None and entry.expire_at <= now:
                # 惰性过期: 堆由 janitor/淘汰路径负责实际 pop, 此处直接删键
                self._remove_locked(key)
                self.stats.expirations += 1
                self.stats.misses += 1
                return None
            entry.referenced = True
            self.stats.hits += 1
            return entry.value

    def set(self, key: bytes, value: bytes, ttl_ms: float, now: float) -> str:
        """返回 stored / replaced / oversized。淘汰过程中目标键也可能被逐出,
        因此每轮都依据分片真实状态重新计算投影, 不缓存旧值。"""
        cost = _ENTRY_FIXED_COST + len(key) + len(value)
        expire_at = None if ttl_ms == 0 else now + ttl_ms / 1000.0
        with self.lock:
            # 单键超过分片总配额 => 无论淘汰多少其他键都放不下, 直接拒绝
            if cost > self.max_bytes:
                self.stats.overflows += 1
                return "oversized"

            def needs() -> tuple[int, int]:
                cur = self.data.get(key)
                cur_cost = cur.cost(len(key)) if cur is not None else 0
                return (
                    self.used_bytes + cost - cur_cost,
                    len(self.data) + (0 if cur is not None else 1),
                )

            need_bytes, need_entries = needs()
            # _evict_one_locked 保证: 分片非空时必逐出一个(内部 hand 指针跨轮
            # 持续推进), 空分片才返回 False。因此外层迭代数 ≤ 现存键数, 有界无活锁。
            loops = 0
            max_loops = len(self.data) + 2
            while (need_bytes > self.max_bytes or need_entries > self.max_entries) and loops < max_loops:
                if not self._evict_one_locked(now):
                    break
                loops += 1
                need_bytes, need_entries = needs()
            if need_bytes > self.max_bytes or need_entries > self.max_entries:
                self.stats.overflows += 1
                return "oversized"
            existed = key in self.data
            if existed:
                self._remove_locked(key)  # 同时把旧键摘出 CLOCK 环
            entry = Entry(value, expire_at)
            self.data[key] = entry
            # 覆盖也视为环上的"新到"键挂到队尾: 更新即续命, 符合 LRU 直觉
            self.ring.append(key)
            self.used_bytes += cost
            self.stats.sets += 1
            if expire_at is not None:
                heapq.heappush(self.expiry, (expire_at, next(_HEAP_SEQ), key))
            return "replaced" if existed else "stored"

    def delete(self, key: bytes, now: float) -> bool:
        with self.lock:
            entry = self.data.get(key)
            if entry is None:
                return False
            if entry.expire_at is not None and entry.expire_at <= now:
                self._remove_locked(key)
                self.stats.expirations += 1
                return False
            self._remove_locked(key)
            return True

    def expire(self, key: bytes, ttl_ms: float, now: float) -> bool:
        with self.lock:
            entry = self.data.get(key)
            if entry is None:
                return False
            if ttl_ms <= 0:
                self._remove_locked(key)
                self.stats.expirations += 1
                return True
            entry.expire_at = now + ttl_ms / 1000.0
            entry.referenced = True
            heapq.heappush(self.expiry, (entry.expire_at, next(_HEAP_SEQ), key))
            return True

    def reap_expired(self, now: float, budget: int = 256) -> int:
        removed = 0
        with self.lock:
            stale_guard = budget + 64
            while removed < budget and self.expiry and stale_guard:
                stale_guard -= 1
                if self.expiry[0][0] > now:
                    break
                if self._pop_expired_locked(now):
                    removed += 1
        return removed

    def flush(self) -> None:
        with self.lock:
            self.data.clear()
            self.ring.clear()
            self.hand_idx = 0
            self.expiry.clear()
            self.used_bytes = 0


class Cache:
    """分片门面: 路由 + 聚合快照 + 后台过期清扫线程。

    每个请求只 hash 路由到一个分片并持有该分片一把锁; janitor 也只是逐片
    加锁, 从不嵌套, 因此系统里不存在任何两把锁同时被持有的路径。"""

    def __init__(self, segments: int = 16, max_bytes: int = 64 * 1 << 20,
                 max_entries: int = 100_000, janitor_interval: float = 0.1) -> None:
        segments = max(1, int(segments))
        self.segment_count = segments
        self._mask = segments - 1
        self.started_at = time.monotonic()
        per_bytes = max(1, max_bytes // segments)
        per_entries = max(1, max_entries // segments)
        self._segments = [_Segment(i, per_bytes, per_entries) for i in range(segments)]
        self.max_bytes = per_bytes * segments
        self.max_entries = per_entries * segments
        self._janitor_interval = janitor_interval
        self._stop = threading.Event()
        self._janitor: threading.Thread | None = None

    def _segment(self, key: bytes) -> _Segment:
        return self._segments[hash(key) & self._mask]

    def start_janitor(self) -> None:
        if self._janitor is not None:
            return
        self._janitor = threading.Thread(
            target=self._janitor_loop, name="cache-janitor", daemon=True)
        self._janitor.start()

    def _janitor_loop(self) -> None:
        while not self._stop.wait(self._janitor_interval):
            now = time.monotonic()
            for seg in self._segments:
                seg.reap_expired(now)

    def close(self) -> None:
        self._stop.set()

    # ----- 键值操作 ------------------------------------------------------

    def get(self, key: bytes) -> bytes | None:
        return self._segment(key).get(key, time.monotonic())

    def set(self, key: bytes, value: bytes, ttl_ms: float = 0.0) -> str:
        return self._segment(key).set(key, value, ttl_ms, time.monotonic())

    def delete(self, key: bytes) -> bool:
        return self._segment(key).delete(key, time.monotonic())

    def expire(self, key: bytes, ttl_ms: float) -> bool:
        return self._segment(key).expire(key, ttl_ms, time.monotonic())

    def flush(self) -> None:
        for seg in self._segments:
            seg.flush()

    def keys(self, limit: int = TCP_KEY_LIMIT) -> list[bytes]:
        out: list[bytes] = []
        now = time.monotonic()
        for seg in self._segments:
            with seg.lock:
                for key, entry in seg.data.items():
                    if entry.expire_at is not None and entry.expire_at <= now:
                        continue
                    out.append(key)
                    if len(out) >= limit:
                        return out
        return out

    def snapshot(self, sample: int = 100) -> dict:
        """逐片短时取锁聚合, 无全局停顿; 各计数之间允许轻微不一致(最终一致)。"""
        totals = AtomicStats()
        seg_info = []
        used = 0
        entries = 0
        samples: list[dict] = []
        now = time.monotonic()
        per_sample = max(1, sample // max(1, self.segment_count))
        for seg in self._segments:
            with seg.lock:
                st = seg.stats
                totals.hits += st.hits
                totals.misses += st.misses
                totals.sets += st.sets
                totals.evictions += st.evictions
                totals.expirations += st.expirations
                totals.overflows += st.overflows
                used += seg.used_bytes
                live = 0
                for key, entry in seg.data.items():
                    if entry.expire_at is not None and entry.expire_at <= now:
                        continue
                    live += 1
                entries += live
                seg_info.append({
                    "segment": seg.index,
                    "entries": len(seg.data),
                    "live_entries": live,
                    "used_bytes": seg.used_bytes,
                    "max_bytes": seg.max_bytes,
                    "hits": st.hits,
                    "misses": st.misses,
                    "evictions": st.evictions,
                    "expirations": st.expirations,
                })
                if len(samples) < sample:
                    for key, entry in list(seg.data.items())[:per_sample]:
                        if entry.expire_at is not None and entry.expire_at <= now:
                            continue
                        samples.append({
                            "segment": seg.index,
                            "key": key.decode("utf-8", "replace"),
                            "size": len(entry.value),
                            "ttl_ms": None if entry.expire_at is None
                            else max(0, int((entry.expire_at - now) * 1000)),
                        })
                        if len(samples) >= sample:
                            break
        lookups = totals.hits + totals.misses
        return {
            "version": VERSION,
            "uptime_seconds": round(time.monotonic() - self.started_at, 3),
            "segments": self.segment_count,
            "entries": entries,
            "used_bytes": used,
            "max_bytes": self.max_bytes,
            "max_entries": self.max_entries,
            "memory_ratio": round(used / self.max_bytes, 6) if self.max_bytes else 0,
            "hits": totals.hits,
            "misses": totals.misses,
            "sets": totals.sets,
            "evictions": totals.evictions,
            "expirations": totals.expirations,
            "overflows": totals.overflows,
            "hit_rate": round(totals.hits / lookups, 6) if lookups else 0.0,
            "qps_lookups": lookups,
            "per_segment": seg_info,
            "samples": samples,
        }


# ---------------------------------------------------------------------------
# TCP 文本协议
# ---------------------------------------------------------------------------

_KEY_RE = re.compile(rb"^[\x21-\x7e]{1,250}$")  # 可见 ASCII, 无空格, 1..250 字节
_HTTP_REQUEST_LINE = re.compile(
    rb"^(?:GET|HEAD|POST|PUT|DELETE|OPTIONS) [\x21-\x7e]* HTTP/1\.[01]\r?\n",
    re.IGNORECASE)


def _encode_value_line(value: bytes) -> bytes:
    return value.replace(b"\\", b"\\\\").replace(b"\r", b"\\r").replace(b"\n", b"\\n")


def handle_tcp_command(cache: Cache, line: bytes) -> bytes:
    """处理一条已去尾换行的命令, 返回待发送字节(不含尾换行)。
    命令字大小写不敏感; SET 为定界解析, 允许 value 含空格。"""
    if not line:
        return b"ERR empty command"
    parts = line.split(b" ", 1)
    cmd = parts[0].upper()
    arg = parts[1] if len(parts) == 2 else b""

    if cmd == b"PING":
        return b"PONG" + (b" " + arg if arg else b"")

    if cmd == b"GET":
        if not arg or b" " in arg:
            return b"ERR usage: GET <key>"
        if not _KEY_RE.match(arg):
            return b"ERR invalid key"
        value = cache.get(arg)
        return b"VALUE " + _encode_value_line(value) if value is not None else b"NOT_FOUND"

    if cmd == b"SET":
        # SET <key> <ttl_seconds> <value...>
        head, _, rest = arg.partition(b" ")
        key = head
        ttl_token, sep, value = rest.partition(b" ")
        if not key or not sep:
            return b"ERR usage: SET <key> <ttl_seconds> <value>"
        if not _KEY_RE.match(key):
            return b"ERR invalid key"
        try:
            ttl_s = float(ttl_token)
        except ValueError:
            return b"ERR ttl must be a non-negative number (seconds; 0 = forever)"
        if ttl_s < 0:
            return b"ERR ttl must be >= 0"
        if not value:
            return b"ERR empty value"
        result = cache.set(key, value, ttl_s * 1000.0)
        return {
            "stored": b"STORED",
            "replaced": b"STORED",
            "oversized": b"SERVER_ERROR object too large for cache",
        }[result]

    if cmd == b"DEL":
        if not arg or b" " in arg or not _KEY_RE.match(arg):
            return b"ERR usage: DEL <key>"
        return b"DELETED" if cache.delete(arg) else b"NOT_FOUND"

    if cmd == b"EXPIRE":
        head, _, ttl_token = arg.partition(b" ")
        key, ttl_token = head, ttl_token.strip()
        if not key or not ttl_token or b" " in ttl_token or not _KEY_RE.match(key):
            return b"ERR usage: EXPIRE <key> <ttl_seconds>"
        try:
            ttl_s = float(ttl_token)
        except ValueError:
            return b"ERR ttl must be a number of seconds"
        return b"OK" if cache.expire(key, ttl_s * 1000.0) else b"NOT_FOUND"

    if cmd == b"KEYS":
        keys = cache.keys(TCP_KEY_LIMIT)
        out = [b"BEGIN " + str(len(keys)).encode()]
        out.extend(b"KEY " + k for k in keys)
        out.append(b"END")
        return b"\n".join(out)

    if cmd == b"STATS":
        snap = cache.snapshot()
        fields = (
            "version", "uptime_seconds", "segments", "entries",
            "used_bytes", "max_bytes", "memory_ratio",
            "hits", "misses", "sets", "evictions", "expirations",
            "overflows", "hit_rate",
        )
        return b"\n".join(
            f"{k}={snap[k]}".encode() for k in fields)

    if cmd == b"FLUSH":
        cache.flush()
        return b"OK"

    return b"ERR unknown command: " + cmd


# ---------------------------------------------------------------------------
# HTTP 仪表盘
# ---------------------------------------------------------------------------

def _human_bytes(n: int) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    size = float(n)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024


def render_dashboard(snap: dict) -> bytes:
    css = (
        "body{font-family:Segoe UI,system-ui,sans-serif;margin:24px;"
        "background:#f6f7f9;color:#222}"
        "h1{font-size:20px}.grid{display:flex;flex-wrap:wrap;gap:12px;margin:16px 0}"
        ".card{background:#fff;border:1px solid #e3e6ea;border-radius:10px;"
        "padding:14px 18px;min-width:150px}"
        ".card b{display:block;font-size:24px;margin-top:4px}"
        "table{border-collapse:collapse;background:#fff;width:100%;margin:8px 0 28px;"
        "border:1px solid #e3e6ea;border-radius:8px}"
        "th,td{padding:7px 10px;text-align:left;border-bottom:1px solid #eef0f3;font-size:13px}"
        "th{background:#fafbfc}"
        ".bar{display:inline-block;width:120px;height:8px;background:#eee;border-radius:4px;"
        "vertical-align:middle;margin-right:8px}"
        ".bar i{display:block;height:100%;background:#4a90d9;border-radius:4px}"
        ".muted{color:#999}code{background:#eef1f4;padding:1px 5px;border-radius:4px}"
    )
    rows = []
    for seg in snap["per_segment"]:
        pct = seg["used_bytes"] / seg["max_bytes"] * 100 if seg["max_bytes"] else 0
        rows.append(
            f"<tr><td>{seg['segment']}</td>"
            f"<td>{seg['live_entries']}/{seg['entries']}</td>"
            f"<td><div class='bar'><i style='width:{pct:.1f}%'></i></div>"
            f"{_human_bytes(seg['used_bytes'])} / {_human_bytes(seg['max_bytes'])} "
            f"({pct:.1f}%)</td>"
            f"<td>{seg['hits']}</td><td>{seg['misses']}</td>"
            f"<td>{seg['evictions']}</td><td>{seg['expirations']}</td></tr>"
        )
    sample_rows = []
    for item in snap["samples"]:
        ttl = "永久" if item["ttl_ms"] is None else f"{item['ttl_ms'] / 1000:.1f}s"
        sample_rows.append(
            f"<tr><td>#{item['segment']}</td>"
            f"<td>{html.escape(item['key'])}</td>"
            f"<td>{_human_bytes(item['size'])}</td><td>{ttl}</td></tr>"
        )
    if not sample_rows:
        sample_rows.append("<tr><td colspan='4' class='muted'>(暂无键)</td></tr>")
    seg_rows = "".join(rows)
    sample_html = "".join(sample_rows)
    doc = (
        "<!doctype html><html lang='zh'><head><meta charset='utf-8'>"
        "<meta http-equiv='refresh' content='2'>"
        "<title>minicache 仪表盘</title>"
        f"<style>{css}</style></head><body>"
        f"<h1>minicache 双协议缓存仪表盘 "
        f"<span class='muted'>每 2 秒自动刷新 · {html.escape(snap['version'])}</span></h1>"
        "<div class='grid'>"
        f"<div class='card'>运行时长<b>{snap['uptime_seconds']}s</b></div>"
        f"<div class='card'>在线键数<b>{snap['entries']}</b></div>"
        f"<div class='card'>命中率<b>{snap['hit_rate']:.2%}</b></div>"
        f"<div class='card'>命中 / 未命中<b>{snap['hits']} / {snap['misses']}</b></div>"
        f"<div class='card'>写入次数<b>{snap['sets']}</b></div>"
        f"<div class='card'>淘汰键数<b>{snap['evictions']}</b></div>"
        f"<div class='card'>过期键数<b>{snap['expirations']}</b></div>"
        f"<div class='card'>拒绝(超大)<b>{snap['overflows']}</b></div>"
        f"<div class='card'>内存占用<b>{_human_bytes(snap['used_bytes'])} / "
        f"{_human_bytes(snap['max_bytes'])}</b></div>"
        f"<div class='card'>分片数<b>{snap['segments']}</b></div>"
        "</div>"
        f"<p class='muted'>JSON 接口: <code>/stats</code> (完整指标) · "
        f"<code>/keys</code> (键列表, 上限 {HTTP_KEY_LIMIT})</p>"
        "<h2>分片状态(分段锁独立计数)</h2>"
        "<table><thead><tr><th>#</th><th>存活/总条目</th><th>内存</th>"
        "<th>命中</th><th>未命中</th><th>CLOCK 淘汰</th><th>过期</th></tr></thead>"
        f"<tbody>{seg_rows}</tbody></table>"
        "<h2>键值抽样(最多 100)</h2>"
        "<table><thead><tr><th>分片</th><th>键</th><th>值大小</th>"
        "<th>剩余 TTL</th></tr></thead>"
        f"<tbody>{sample_html}</tbody></table>"
        "</body></html>"
    )
    return doc.encode("utf-8")


def handle_http_request(cache: Cache, raw: bytes) -> tuple[bytes, bytes, str]:
    """极简 HTTP/1.1: 仅支持 GET, 始终 Connection: close。
    返回 (status_line, body, content_type)。"""
    try:
        head = raw.split(b"\r\n\r\n", 1)[0]
        lines = head.split(b"\r\n")
        method, target, _ = lines[0].split(b" ", 2)
    except (ValueError, IndexError):
        body = b"<h1>400 Bad Request</h1>"
        return b"HTTP/1.1 400 Bad Request", body, "text/html; charset=utf-8"
    path = target.split(b"?", 1)[0].decode("latin1")
    if method != b"GET":
        body = b"<h1>405 Method Not Allowed</h1>"
        return b"HTTP/1.1 405 Method Not Allowed", body, "text/html; charset=utf-8"
    if path == "/" or path == "/index.html":
        body = render_dashboard(cache.snapshot())
        ctype = "text/html; charset=utf-8"
        status = b"HTTP/1.1 200 OK"
    elif path == "/stats":
        body = json.dumps(cache.snapshot(), ensure_ascii=False, indent=2).encode("utf-8")
        ctype = "application/json; charset=utf-8"
        status = b"HTTP/1.1 200 OK"
    elif path == "/keys":
        keys = [k.decode("utf-8", "replace") for k in cache.keys(HTTP_KEY_LIMIT)]
        body = json.dumps({"count": len(keys), "keys": keys},
                          ensure_ascii=False, indent=2).encode("utf-8")
        ctype = "application/json; charset=utf-8"
        status = b"HTTP/1.1 200 OK"
    else:
        body = b"<h1>404 Not Found</h1>"
        ctype = "text/html; charset=utf-8"
        status = b"HTTP/1.1 404 Not Found"
    return status, body, ctype


# ---------------------------------------------------------------------------
# 单线程 selectors 双协议服务器
# ---------------------------------------------------------------------------


class Connection:
    __slots__ = ("sock", "addr", "inbuf", "outbuf", "is_http",
                 "closing", "http_parsed")

    def __init__(self, sock: socket.socket, addr: tuple) -> None:
        self.sock = sock
        self.addr = addr
        self.inbuf = bytearray()
        self.outbuf = bytearray()
        self.is_http: bool | None = None   # None=未嗅探, True/False=已定型
        self.closing = False
        self.http_parsed = False


class CacheServer:
    """一个监听端口、一个事件循环线程同时承载 HTTP 与 TCP 文本协议。

    协议嗅探: 连接首字节为 ASCII 字母 => 可能是 HTTP(以 GET/HEAD 开头);
    收到 "HTTP/x.x" 尾标记才判定 HTTP, 否则按 TCP 处理(GET/SET 也是字母开头,
    但不会出现 HTTP/ 版本标记)。"""

    def __init__(self, cache: Cache, host: str = "127.0.0.1", port: int = DEFAULT_PORT,
                 max_clients: int = 1024) -> None:
        self.cache = cache
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self._selector = selectors.DefaultSelector()
        self._server: socket.socket | None = None
        self._clients: dict[socket.socket, Connection] = {}
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._shutdown_done = threading.Event()

    @property
    def address(self) -> tuple[str, int]:
        assert self._server is not None
        return self._server.getsockname()[:2]

    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(128)
        srv.setblocking(False)
        self._server = srv
        self._selector.register(srv, selectors.EVENT_READ, None)
        self._thread = threading.Thread(target=self.serve_forever, name="cache-io", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        # 唤醒可能正阻塞在 select 上的 IO 线程
        try:
            if self._server is not None:
                self._server.close()
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=3)

    def serve_forever(self) -> None:
        sel = self._selector
        while not self._stopping.is_set():
            try:
                events = sel.select(timeout=0.5)
            except (OSError, ValueError):
                break
            for key, mask in events:
                conn = key.data
                try:
                    if conn is None:
                        self._accept()
                    else:
                        if mask & selectors.EVENT_READ:
                            self._readable(conn)
                        if mask & selectors.EVENT_WRITE:
                            self._writable(conn)
                except Exception:
                    # 单连接的任何异常(含协议解析 bug)都被隔离在该连接内,
                    # 绝不让一个坏客户端杀死整个事件循环线程
                    self._close_conn(conn if conn is not None else None)
        self._shutdown_all()

    # ----- 接受连接 ------------------------------------------------------

    def _accept(self) -> None:
        assert self._server is not None
        try:
            sock, addr = self._server.accept()
        except (BlockingIOError, OSError):
            return
        if len(self._clients) >= self.max_clients:
            sock.close()  # 超额直接拒绝, 保护进程
            return
        sock.setblocking(False)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        conn = Connection(sock, addr)
        self._clients[sock] = conn
        self._selector.register(sock, selectors.EVENT_READ, conn)

    # ----- 读路径 --------------------------------------------------------

    def _readable(self, conn: Connection) -> None:
        try:
            chunk = conn.sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return
        if not chunk:
            self._half_close(conn)
            return
        conn.inbuf.extend(chunk)
        if len(conn.inbuf) > MAX_LINE_BYTES and conn.is_http is not True:
            # 协议行 / 请求头超长, 立即丢弃连接, 防止恶意客户端撑爆内存
            self._close_conn(conn)
            return
        if conn.is_http is None:
            sniff = bytes(conn.inbuf)
            nl = sniff.find(b"\n")
            head = sniff if nl < 0 else sniff[:nl + 1]
            if _HTTP_REQUEST_LINE.match(head):
                # 严格形如 "GET /path HTTP/1.1\r\n"; TCP 命令不会有版本尾标记
                conn.is_http = True
            elif nl >= 0:
                # TCP 文本协议以换行成帧
                conn.is_http = False
            elif len(sniff) > MAX_LINE_BYTES:
                self._close_conn(conn)
                return
        if conn.is_http:
            self._pump_http(conn)
        elif conn.is_http is False:
            self._pump_tcp(conn)

    def _pump_tcp(self, conn: Connection) -> None:
        buf = conn.inbuf
        while True:
            nl = buf.find(b"\n")
            if nl < 0:
                if len(buf) > MAX_LINE_BYTES:
                    self._close_conn(conn)
                    return
                break
            line = bytes(buf[:nl]).rstrip(b"\r")
            del buf[:nl + 1]
            reply = handle_tcp_command(self.cache, line)
            self._queue_send(conn, reply + b"\n")
            # _queue_send 在背压超限时可能已关闭并注销该连接
            if conn.closing or conn.sock not in self._clients:
                return

    def _pump_http(self, conn: Connection) -> None:
        if conn.http_parsed:
            return
        if b"\r\n\r\n" not in conn.inbuf and b"\n\n" not in conn.inbuf:
            if len(conn.inbuf) > MAX_LINE_BYTES:
                self._close_conn(conn)
            return
        conn.http_parsed = True
        status, body, ctype = handle_http_request(self.cache, bytes(conn.inbuf))
        headers = (
            status + b"\r\n"
            b"Content-Type: " + ctype.encode() + b"\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"Server: " + VERSION.encode() + b"\r\n\r\n"
        )
        self._queue_send(conn, headers + body)
        conn.closing = True

    # ----- 写路径(背压安全) ----------------------------------------------

    def _queue_send(self, conn: Connection, payload: bytes) -> None:
        if len(conn.outbuf) + len(payload) > MAX_OUTPUT_BYTES:
            self._close_conn(conn)
            return
        conn.outbuf.extend(payload)
        self._update_events(conn)

    def _writable(self, conn: Connection) -> None:
        if not conn.outbuf:
            self._update_events(conn)
            return
        try:
            sent = conn.sock.send(conn.outbuf)
        except (BlockingIOError, InterruptedError):
            return
        del conn.outbuf[:sent]
        if not conn.outbuf and conn.closing:
            self._close_conn(conn)
            return
        self._update_events(conn)

    def _update_events(self, conn: Connection) -> None:
        mask = selectors.EVENT_READ
        if conn.outbuf:
            mask |= selectors.EVENT_WRITE
        try:
            self._selector.modify(conn.sock, mask, conn)
        except (KeyError, ValueError):
            self._close_conn(conn)

    def _half_close(self, conn: Connection) -> None:
        if conn.outbuf:
            conn.closing = True
            try:
                self._selector.modify(conn.sock, selectors.EVENT_WRITE, conn)
            except (KeyError, ValueError):
                self._close_conn(conn)
        else:
            self._close_conn(conn)

    def _close_conn(self, conn: Connection | None) -> None:
        if conn is None:
            return
        try:
            self._selector.unregister(conn.sock)
        except (KeyError, ValueError):
            pass
        try:
            conn.sock.close()
        except OSError:
            pass
        self._clients.pop(conn.sock, None)

    def _shutdown_all(self) -> None:
        # stop() 与 IO 线程都可能走到这里; 用事件保证清理严格只执行一次
        if self._shutdown_done.is_set():
            return
        self._shutdown_done.set()
        for conn in list(self._clients.values()):
            self._close_conn(conn)
        if self._server is not None:
            try:
                self._selector.unregister(self._server)
            except (KeyError, ValueError):
                pass
            try:
                self._server.close()
            except OSError:
                pass
            self._server = None
        self._selector.close()


# ---------------------------------------------------------------------------
# 内置后台随机读写压测
# ---------------------------------------------------------------------------


class LoadGenerator:
    """后台压测: 多操作混合, 部分键带短 TTL, 持续制造命中/未命中/过期/淘汰。
    直接调用引擎内存接口(不走网络), 专注验证并发存储路径。"""

    def __init__(self, cache: Cache, keyspace: int = 400, value_size: int = 96,
                 interval: float = 0.0) -> None:
        self.cache = cache
        self.keyspace = keyspace
        self.value_size = value_size
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="cache-load", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        rng = random.Random(0xC0FFEE)
        counter = 0
        while not self._stop.is_set():
            counter += 1
            roll = rng.random()
            key = f"load:{rng.randrange(self.keyspace):04d}:{counter % 9}".encode()
            if roll < 0.60:                      # GET
                self.cache.get(key)
            elif roll < 0.90:                    # SET, 约 1/4 带短 TTL
                ttl = 0.0
                if rng.random() < 0.25:
                    ttl = rng.uniform(0.05, 2.0) * 1000.0
                value = (str(counter) + ":" + "x" * self.value_size).encode()
                self.cache.set(key, value, ttl)
            elif roll < 0.95:                    # EXPIRE 抖动
                self.cache.expire(key, rng.uniform(0.05, 1.5) * 1000.0)
            else:                                # DEL
                self.cache.delete(key)
            if self.interval:
                self._stop.wait(self.interval)


# ---------------------------------------------------------------------------
# 内置测试: 引擎级并发一致性
# ---------------------------------------------------------------------------


class EngineBasicTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = Cache(segments=4, max_bytes=1 << 20, max_entries=10_000)

    def test_set_get_delete(self) -> None:
        self.assertEqual(self.cache.set(b"k1", b"v1"), "stored")
        self.assertEqual(self.cache.get(b"k1"), b"v1")
        self.assertEqual(self.cache.set(b"k1", b"v2", 0.0), "replaced")
        self.assertEqual(self.cache.get(b"k1"), b"v2")
        self.assertTrue(self.cache.delete(b"k1"))
        self.assertIsNone(self.cache.get(b"k1"))
        self.assertFalse(self.cache.delete(b"k1"))

    def test_value_with_spaces_and_newline(self) -> None:
        self.cache.set(b"k", b"a b c\nd\\e", 0)
        self.assertEqual(handle_tcp_command(self.cache, b"GET k"),
                         b"VALUE a b c\\nd\\\\e")

    def test_miss_and_snapshot_counters(self) -> None:
        self.assertIsNone(self.cache.get(b"nope"))
        self.cache.set(b"a", b"1", 0)
        self.cache.get(b"a")
        snap = self.cache.snapshot()
        self.assertEqual(snap["hits"], 1)
        self.assertEqual(snap["misses"], 1)
        self.assertEqual(snap["sets"], 1)
        self.assertEqual(snap["entries"], 1)
        self.assertAlmostEqual(snap["hit_rate"], 0.5)

    def test_flush_clears_data_only(self) -> None:
        self.cache.set(b"a", b"1", 0)
        self.cache.get(b"a")
        self.cache.flush()
        snap = self.cache.snapshot()
        self.assertEqual(snap["entries"], 0)
        self.assertEqual(snap["used_bytes"], 0)
        self.assertEqual(snap["hits"], 1)  # 计数器保留


class EngineTTLTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = Cache(segments=4, max_bytes=1 << 20, max_entries=10_000,
                           janitor_interval=0.02)

    def test_lazy_expiry(self) -> None:
        self.cache.set(b"t", b"v", 30)  # 30ms
        self.assertEqual(self.cache.get(b"t"), b"v")
        time.sleep(0.08)
        self.assertIsNone(self.cache.get(b"t"))
        snap = self.cache.snapshot()
        self.assertEqual(snap["entries"], 0)
        self.assertGreaterEqual(snap["expirations"], 1)

    def test_janitor_reaps_expired(self) -> None:
        self.cache.start_janitor()
        try:
            for i in range(50):
                self.cache.set(f"t{i}".encode(), b"x", 20)
            time.sleep(0.15)
            self.assertEqual(self.cache.snapshot()["entries"], 0)
        finally:
            self.cache.close()

    def test_expire_command_refresh_and_kill(self) -> None:
        self.cache.set(b"t", b"v", 0)
        self.assertTrue(self.cache.expire(b"t", 100_000))
        self.assertEqual(self.cache.get(b"t"), b"v")
        self.assertTrue(self.cache.expire(b"t", 0))
        self.assertIsNone(self.cache.get(b"t"))
        self.assertFalse(self.cache.expire(b"missing", 1000))

    def test_renew_ttl_via_set(self) -> None:
        self.cache.set(b"t", b"v1", 20)
        time.sleep(0.05)
        self.assertIsNone(self.cache.get(b"t"))
        self.cache.set(b"t", b"v2", 100_000)
        time.sleep(0.05)
        self.assertEqual(self.cache.get(b"t"), b"v2")


class EngineEvictionTests(unittest.TestCase):
    def test_clock_second_chance_keeps_recently_used(self) -> None:
        # 确定性时序验证 CLOCK 二次机会:
        # 容量3, a/b/c 填满(位全1); set d 触发首次淘汰 => 整轮降级后逐出最老的 a,
        # 环变为 [b c d](b/c 位0, d 新入位1), 游标指向 b。
        cache = Cache(segments=1, max_bytes=1 << 22, max_entries=3)
        cache.set(b"a", b"1", 0)
        cache.set(b"b", b"2", 0)
        cache.set(b"c", b"3", 0)
        cache.set(b"d", b"4", 0)
        self.assertIsNone(cache.get(b"a"))
        # 在淘汰扫描即将到达 b 前热读它: b 置位。set e 触发第二次淘汰,
        # 扫描 b(降级, 给二次机会) -> c(位0) => 逐出 c, b 存活
        cache.get(b"b")
        cache.set(b"e", b"5", 0)
        self.assertEqual(cache.get(b"b"), b"2")  # 被热读, 二次机会存活
        self.assertIsNone(cache.get(b"c"))      # 最冷, 被逐出
        self.assertEqual(cache.get(b"d"), b"4")
        self.assertEqual(cache.get(b"e"), b"5")
        self.assertEqual(cache.snapshot()["evictions"], 2)

    def test_clock_ring_integrity_under_overwrite_and_delete(self) -> None:
        # 反复覆盖同一键 + 随机删除, 环与游标必须始终自洽, 淘汰永不卡死
        cache = Cache(segments=1, max_bytes=1 << 22, max_entries=8)
        for i in range(8):
            cache.set(f"k{i}".encode(), b"v", 0)
        for rep in range(200):
            cache.set(f"k{rep % 8}".encode(), f"v{rep}".encode(), 0)
            if rep % 5 == 0:
                cache.delete(f"k{(rep + 3) % 8}".encode())
                cache.set(f"new{rep}".encode(), b"v", 0)
        seg = cache._segments[0]
        self.assertEqual(len(seg.ring), len(seg.data))
        self.assertEqual(set(seg.ring), set(seg.data))
        self.assertLess(seg.hand_idx, len(seg.ring))

    def test_approximate_lru_ordering_property(self) -> None:
        # 从同一稳定起点(所有 referenced 清零)出发: 一半键持续热读, 另一半
        # 从不访问; 制造恰好半容量的淘汰后, 热键应被二次机会保护而冷键被逐出。
        cache = Cache(segments=1, max_bytes=1 << 22, max_entries=100)
        for i in range(100):
            cache.set(f"k{i:03d}".encode(), b"v", 0)
        for entry in cache._segments[0].data.values():
            entry.referenced = False
        hot = [f"k{i:03d}".encode() for i in range(0, 100, 2)]
        cold = [f"k{i:03d}".encode() for i in range(1, 100, 2)]
        for _ in range(3):
            for key in hot:
                cache.get(key)
        for i in range(50):
            cache.set(f"new{i:03d}".encode(), b"v", 0)
        hot_survivors = sum(1 for key in hot if cache.get(key) is not None)
        cold_survivors = sum(1 for key in cold if cache.get(key) is not None)
        self.assertEqual(hot_survivors, 50)
        self.assertEqual(cold_survivors, 0)

    def test_memory_cap_no_overshoot(self) -> None:
        cache = Cache(segments=2, max_bytes=4_000, max_entries=1_000)
        for i in range(200):
            cache.set(f"k{i:04d}".encode(), b"x" * 200, 0)
        snap = cache.snapshot()
        self.assertLessEqual(snap["used_bytes"], snap["max_bytes"])
        self.assertLessEqual(snap["entries"], snap["max_entries"])
        self.assertGreater(snap["evictions"], 0)

    def test_oversized_single_entry_rejected(self) -> None:
        cache = Cache(segments=1, max_bytes=1_000, max_entries=10)
        self.assertEqual(cache.set(b"big", b"x" * 2_000, 0), "oversized")
        self.assertIsNone(cache.get(b"big"))
        self.assertEqual(cache.snapshot()["overflows"], 1)

    def test_expired_evicted_before_lru(self) -> None:
        cache = Cache(segments=1, max_bytes=1 << 22, max_entries=2)
        cache.set(b"hot", b"v", 100_000)
        cache.get(b"hot")
        cache.set(b"cold", b"v", 10)  # 很快过期
        time.sleep(0.05)
        cache.set(b"new", b"v", 0)   # 容量满: 优先清掉过期 cold
        self.assertEqual(cache.get(b"hot"), b"v")
        self.assertEqual(cache.get(b"new"), b"v")


class EngineConcurrencyTests(unittest.TestCase):
    def test_parallel_mixed_workload_consistency(self) -> None:
        cache = Cache(segments=8, max_bytes=50_000, max_entries=10_000,
                      janitor_interval=0.02)
        cache.start_janitor()
        errors: list[BaseException] = []
        stop = threading.Event()

        def worker(worker_id: int) -> None:
            try:
                rng = random.Random(worker_id)
                while not stop.is_set():
                    key = f"w{worker_id % 6}:{rng.randrange(120)}".encode()
                    roll = rng.random()
                    if roll < 0.45:
                        cache.get(key)
                    elif roll < 0.8:
                        cache.set(key, b"y" * rng.randrange(10, 400),
                                  rng.choice((0, 0, 0, rng.uniform(5, 500))))
                    elif roll < 0.92:
                        cache.expire(key, rng.uniform(5, 800))
                    else:
                        cache.delete(key)
            except BaseException as exc:  # 捕获一切, 测试线程不应静默死亡
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
        for t in threads:
            t.start()
        time.sleep(2.0)
        stop.set()
        for t in threads:
            t.join(timeout=5)
        for t in threads:
            self.assertFalse(t.is_alive(), "工作线程死锁未退出")
        self.assertEqual(errors, [])
        snap = cache.snapshot()
        self.assertLessEqual(snap["used_bytes"], snap["max_bytes"])
        self.assertLessEqual(snap["entries"], snap["max_entries"])
        # 账本核对: 逐片实际占用必须等于 used_bytes
        total = 0
        now = time.monotonic()
        for seg in cache._segments:
            with seg.lock:
                total += sum(e.cost(len(k)) for k, e in seg.data.items()
                             if not (e.expire_at is not None and e.expire_at <= now))
        self.assertLessEqual(abs(total - snap["used_bytes"]), 8 * _ENTRY_FIXED_COST)
        cache.close()

    def test_invariant_counter_accounting(self) -> None:
        cache = Cache(segments=4, max_bytes=20_000, max_entries=1_000)
        n = 8

        def writer(worker_id: int) -> None:
            for i in range(500):
                cache.set(f"k{worker_id}-{i % 40}".encode(),
                          b"z" * ((i % 50) + 50), 0)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        snap = cache.snapshot()
        # 最终唯一键数受容量约束但计数器精确: sets == 每片 sets 之和
        seg_sets = sum(s["evictions"] for s in snap["per_segment"])
        self.assertEqual(seg_sets, snap["evictions"])
        self.assertEqual(snap["sets"], n * 500)


# ---------------------------------------------------------------------------
# 内置测试: 真实端口双协议网络自测
# ---------------------------------------------------------------------------


class _TcpClient:
    """行文本协议测试助手; 持久连接 + 独立读缓冲。"""

    def __init__(self, host: str, port: int) -> None:
        self.sock = socket.create_connection((host, port), timeout=3)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._buf = b""

    def close(self) -> None:
        self.sock.close()

    def _readline(self) -> bytes:
        while b"\n" not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("连接被关闭")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\n", 1)
        return line.rstrip(b"\r")

    def cmd(self, text: str | bytes) -> bytes:
        if isinstance(text, str):
            text = text.encode()
        self.sock.sendall(text + b"\n")
        return self._readline()

    def keys(self) -> list[bytes]:
        self.sock.sendall(b"KEYS\n")
        first = self._readline()
        count = int(first.split(b" ", 1)[1])
        out = []
        for _ in range(count):
            line = self._readline()
            out.append(line[4:])
        self.assertEqualEnd(self._readline(), b"END")
        return out

    @staticmethod
    def assertEqualEnd(a: bytes, b: bytes) -> None:
        assert a == b, (a, b)


class NetworkProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cache = Cache(segments=4, max_bytes=2 << 20, max_entries=50_000)
        cls.cache.start_janitor()
        cls.server = CacheServer(cls.cache, host="127.0.0.1", port=0)
        cls.server.start()
        cls.host, cls.port = cls.server.address
        time.sleep(0.05)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()
        cls.cache.close()

    def _http(self, path: str) -> tuple[int, bytes, str]:
        url = f"http://{self.host}:{self.port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                return resp.status, resp.read(), resp.headers.get("Content-Type", "")
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers.get("Content-Type", "")

    def test_tcp_ping_set_get_del(self) -> None:
        c = _TcpClient(self.host, self.port)
        try:
            self.assertEqual(c.cmd("PING"), b"PONG")
            self.assertEqual(c.cmd("PING hi"), b"PONG hi")
            self.assertEqual(c.cmd("GET net:k1"), b"NOT_FOUND")
            self.assertEqual(c.cmd("SET net:k1 0 hello world"), b"STORED")
            self.assertEqual(c.cmd("GET net:k1"), b"VALUE hello world")
            self.assertEqual(c.cmd("DEL net:k1"), b"DELETED")
            self.assertEqual(c.cmd("GET net:k1"), b"NOT_FOUND")
            self.assertEqual(c.cmd("DEL net:k1"), b"NOT_FOUND")
            self.assertTrue(c.cmd("BOGUS").startswith(b"ERR unknown command"))
        finally:
            c.close()

    def test_tcp_ttl_expiration_end_to_end(self) -> None:
        c = _TcpClient(self.host, self.port)
        try:
            self.assertEqual(c.cmd("SET net:ttl 0.05 gone-soon"), b"STORED")
            self.assertEqual(c.cmd("GET net:ttl"), b"VALUE gone-soon")
            time.sleep(0.15)
            self.assertEqual(c.cmd("GET net:ttl"), b"NOT_FOUND")
            self.assertEqual(c.cmd("SET net:ttl -1 x"),
                             b"ERR ttl must be >= 0")
            self.assertEqual(c.cmd("SET net:ttl abc x"),
                             b"ERR ttl must be a non-negative number (seconds; 0 = forever)")
        finally:
            c.close()

    def test_tcp_expire_and_stats(self) -> None:
        c = _TcpClient(self.host, self.port)
        try:
            c.cmd("SET net:exp 0 keep")
            self.assertEqual(c.cmd("EXPIRE net:exp 0.05"), b"OK")
            self.assertEqual(c.cmd("EXPIRE net:nope 1"), b"NOT_FOUND")
            stats = {}
            c.sock.sendall(b"STATS\n")
            for _ in range(14):
                line = c._readline()
                k, _, v = line.partition(b"=")
                stats[k.decode()] = v
            self.assertIn("hit_rate", stats)
            self.assertEqual(stats["version"], VERSION.encode())
            time.sleep(0.12)
            self.assertEqual(c.cmd("GET net:exp"), b"NOT_FOUND")
        finally:
            c.close()

    def test_tcp_keys_frame(self) -> None:
        c = _TcpClient(self.host, self.port)
        try:
            c.cmd("FLUSH")
            c.cmd("SET net:a 0 1")
            c.cmd("SET net:b 0 2 2")
            keys = c.keys()
            self.assertEqual(sorted(keys), [b"net:a", b"net:b"])
        finally:
            c.close()

    def test_http_dashboard_and_json(self) -> None:
        status, body, ctype = self._http("/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", ctype)
        self.assertIn("minicache", body.decode("utf-8"))
        status, body, ctype = self._http("/stats")
        self.assertEqual(status, 200)
        self.assertIn("application/json", ctype)
        data = json.loads(body)
        for key in ("hits", "misses", "evictions", "per_segment", "used_bytes",
                    "hit_rate", "samples"):
            self.assertIn(key, data)
        self.assertEqual(len(data["per_segment"]), self.cache.segment_count)
        status, body, _ = self._http("/keys")
        self.assertEqual(status, 200)
        self.assertIn("keys", json.loads(body))
        status, _, _ = self._http("/missing")
        self.assertEqual(status, 404)

    def test_concurrent_network_clients(self) -> None:
        errors: list[BaseException] = []
        stop = threading.Event()

        def client(worker_id: int) -> None:
            c = _TcpClient(self.host, self.port)
            try:
                i = 0
                while not stop.is_set():
                    key = f"net:c{worker_id}:{i % 30}"
                    rep = c.cmd(f"SET {key} 0 v{i}")
                    if rep != b"STORED":
                        raise AssertionError(rep)
                    if c.cmd(f"GET {key}") != f"VALUE v{i}".encode():
                        raise AssertionError("读写不一致")
                    i += 1
            except BaseException as exc:
                errors.append(exc)
            finally:
                c.close()

        threads = [threading.Thread(target=client, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        time.sleep(1.5)
        stop.set()
        for t in threads:
            t.join(timeout=5)
        for t in threads:
            self.assertFalse(t.is_alive())
        self.assertEqual(errors, [])

    def test_malformed_http_returns_400_and_loop_survives(self) -> None:
        # 解析器单元级: 无法解析的 HTTP 字节必须回 400
        status, _, _ = handle_http_request(self.cache, b"only two-segments\r\n\r\n")
        self.assertEqual(status, b"HTTP/1.1 400 Bad Request")
        # 网络级: 畸形/未知请求行得到 4xx, 且连接关闭后事件循环照常服务
        raw = socket.create_connection((self.host, self.port), timeout=3)
        raw.settimeout(2)
        try:
            raw.sendall(b"GET  HTTP/1.1\r\n\r\n")  # 空目标 => 404
            reply = raw.recv(128)
            self.assertIn(reply[:12], (b"HTTP/1.1 400", b"HTTP/1.1 404"))
        finally:
            raw.close()
        # 畸形请求绝不能拖垮事件循环: 紧随其后的 TCP 请求必须正常
        c = _TcpClient(self.host, self.port)
        try:
            self.assertEqual(c.cmd("PING after-bad-http"), b"PONG after-bad-http")
        finally:
            c.close()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="纯标准库双协议内存缓存原型")
    p.add_argument("--host", default="127.0.0.1", help="监听地址 (默认 127.0.0.1)")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help="监听端口 (默认 9900)")
    p.add_argument("--segments", type=int, default=16, help="锁分片数, 内部取 >= 输入的 2 次幂")
    p.add_argument("--max-bytes", type=int, default=64 * 1024 * 1024, help="总内存上限(字节)")
    p.add_argument("--max-entries", type=int, default=100_000, help="总键数上限")
    p.add_argument("--no-load", action="store_true", help="关闭内置后台压测")
    p.add_argument("--test", action="store_true", help="运行内置并发/网络单测后退出")
    return p


def run_tests() -> bool:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite([
        loader.loadTestsFromTestCase(EngineBasicTests),
        loader.loadTestsFromTestCase(EngineTTLTests),
        loader.loadTestsFromTestCase(EngineEvictionTests),
        loader.loadTestsFromTestCase(EngineConcurrencyTests),
        loader.loadTestsFromTestCase(NetworkProtocolTests),
    ])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.test:
        return 0 if run_tests() else 1

    # segments 取 >= 输入的最小 2 次幂, 保证 mask 路由正确
    segs = 1
    while segs < args.segments:
        segs <<= 1
    cache = Cache(segments=segs, max_bytes=args.max_bytes, max_entries=args.max_entries)
    cache.start_janitor()
    server = CacheServer(cache, host=args.host, port=args.port)
    server.start()
    load: LoadGenerator | None = None
    if not args.no_load:
        load = LoadGenerator(cache)
        load.start()

    bound_host, bound_port = server.address
    print("=" * 64)
    print(f" {VERSION} 已启动  (Ctrl+C 退出)")
    print(f" 监听:       {bound_host}:{bound_port}")
    print(f" 分片数:     {cache.segment_count}  (每片独立锁, 无嵌套)")
    print(f" 容量:       {_human_bytes(cache.max_bytes)} / {cache.max_entries} 键")
    print(f" 仪表盘:     http://{bound_host}:{bound_port}/")
    print(f" JSON 指标:  http://{bound_host}:{bound_port}/stats")
    print(f" TCP 示例:   GET key / SET key 60 value / STATS / KEYS")
    print(f" 后台压测:   {'开启' if load else '关闭 (--no-load)'}")
    print("=" * 64, flush=True)

    stop_event = threading.Event()
    import signal

    def _request_stop(signum, frame):  # noqa: ANN001
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
    except (ValueError, OSError):
        pass  # 非主线程环境无法注册信号

    try:
        while not stop_event.wait(0.5):
            pass
    finally:
        if load is not None:
            load.stop()
        server.stop()
        cache.close()
    print("已停止。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
