#!/usr/bin/env python3
"""mcached - dependency-free, dual-protocol multiplexed in-memory cache.

Features
--------
* Segmented storage engine: one independent lock per shard -> no global lock.
* TTL support: lazy expiration on access + chunked background sweeper that
  never holds a shard lock for long.
* CLOCK (second-chance) approximate LRU eviction: O(1) amortised, bounded
  scans, fully thread safe, no over-capacity penetration.
* One selectors-based reactor multiplexes the raw TCP text protocol and HTTP
  (auto-detected on the same port, plus a dedicated HTTP port). Slow clients
  never consume worker threads.
* Background random read/write stress threads and live atomic metrics
  (hit/miss, evictions, expirations, memory bytes, per-segment counters).
* Built-in unittest concurrency / consistency suite (--selftest).

Examples
--------
    python mcached.py                  # serve + background stress
    python mcached.py --port 7723 --http-port 8080 --capacity 200000
    python mcached.py --selftest       # unit + concurrency tests

Text protocol (LF or CRLF separated, pipelining allowed)::

    PING
    GET key
    SET key seconds value ...         # seconds = 0 means keep forever
    DELETE key
    FLUSH
    STATS
    SHUTDOWN
"""

from __future__ import annotations

import argparse
import html
import json
import os
import random
import selectors
import socket
import sys
import threading
import time
import unittest
import zlib
from collections import deque


# ---------------------------------------------------------------------------
# Storage engine: segmented hash map + CLOCK approximate LRU + TTL
# ---------------------------------------------------------------------------

_ENTRY_OVERHEAD = sys.getsizeof(["", 0.0, 0])


class Segment:
    """One locked shard: dict store, CLOCK ring, and local counters.

    Deadlock-freedom rule: every public cache operation hashes to exactly one
    segment and holds at most one segment lock at a time. There is no global
    store lock and no lock ordering across shards, so deadlock is impossible.
    """

    __slots__ = (
        "lock", "items", "ring", "clock_hand", "cap", "live_bytes",
        "hits", "misses", "sets", "deletes", "evictions", "expirations",
        "sweep_keys",
    )

    def __init__(self, cap):
        self.lock = threading.Lock()
        self.items = {}
        self.ring = deque()
        self.clock_hand = 0
        self.cap = cap
        self.live_bytes = 0
        self.hits = 0
        self.misses = 0
        self.sets = 0
        self.deletes = 0
        self.evictions = 0
        self.expirations = 0
        self.sweep_keys = []


class SegmentedCache:
    """Thread-safe cache sharded into independently locked segments.

    Concurrency design
    ------------------
    * Each shard has its own lock; every get/set/delete touches exactly one
      shard and therefore holds at most one shard lock -> no global store
      lock, no lock ordering, deadlock-free by construction.
    * A :class:`BoundedSemaphore` with ``capacity`` permits is the single
      global *admission* point: a brand-new key acquires one permit before it
      is inserted, so total live items can never exceed capacity (no
      overshoot/penetration), even under heavy concurrent writers.
    * Eviction/expiry release the permit back. Permits are an atomic C
      operation with almost no contention compared with a data lock.
    * Shard *soft* caps (+slack) drive the CLOCK approximate-LRU scan so the
      common case stays local; if one shard overflows from hash skew, other
      shards are scanned independently (never while holding a shard lock).
    """

    def __init__(self, capacity=100000, segments=16):
        if capacity < 1:
            raise ValueError("capacity must be positive")
        capacity = int(capacity)
        segments = max(1, min(int(segments), capacity))
        self.segment_count = segments
        self.capacity = capacity
        self._permits = threading.BoundedSemaphore(capacity)
        # BoundedSemaphore has no public remaining counter; keep one guarded
        # only by itself (rebuilt atomically on flush).
        self._permit_lock = threading.Lock()
        self._permits_left = capacity
        soft = max(1, int(capacity * 1.25 // segments) + 2)
        self.segments = [Segment(soft) for _ in range(segments)]
        self.created = time.monotonic()

    def _segment(self, key):
        idx = zlib.crc32(key.encode("utf-8")) % self.segment_count
        return self.segments[idx]

    @staticmethod
    def _entry_size(key, value):
        return _ENTRY_OVERHEAD + len(key.encode("utf-8")) + len(value.encode("utf-8"))

    @staticmethod
    def _expired(entry, now):
        return entry[1] != 0.0 and entry[1] <= now

    # -- permit accounting (capacity admission) ---------------------------

    def _acquire_permit(self):
        if self._permits.acquire(blocking=False):
            with self._permit_lock:
                self._permits_left -= 1
            return True
        return False

    def _release_permit(self):
        with self._permit_lock:
            if self._permits_left >= self.capacity:
                return  # can happen only after a flush rebuild
            self._permits_left += 1
        self._permits.release()

    def get(self, key):
        """Return value or None; lazy-expire and mark the CLOCK ref bit."""
        seg = self._segment(key)
        with seg.lock:
            entry = seg.items.get(key)
            if entry is None:
                seg.misses += 1
                return None
            now = time.monotonic()
            if self._expired(entry, now):
                self._drop_locked(seg, key, "expiration")
                seg.misses += 1
                return None
            entry[2] = 1
            seg.hits += 1
            return entry[0]

    def set(self, key, value, ttl=0.0):
        now = time.monotonic()
        expire_at = 0.0 if ttl <= 0 else now + float(ttl)
        seg = self._segment(key)

        with seg.lock:
            old = seg.items.get(key)
            if old is not None and not self._expired(old, now):
                seg.live_bytes += (self._entry_size(key, value)
                                   - self._entry_size(key, old[0]))
                old[0] = value
                old[1] = expire_at
                old[2] = 1
                seg.sets += 1
                return
            # Either absent or expired. Remember whether an expired entry
            # occupies a permit; it will be reclaimed during admission.
            expired_old = old is not None
        if expired_old:
            with seg.lock:
                still = seg.items.get(key)
                if still is not None and self._expired(still, time.monotonic()):
                    self._drop_locked(seg, key, "expiration")
                    expired_old = True
                else:
                    expired_old = False
            if not expired_old:
                with seg.lock:
                    live = seg.items.get(key)
                    if live is not None:
                        seg.live_bytes += (self._entry_size(key, value)
                                           - self._entry_size(key, live[0]))
                        live[0] = value
                        live[1] = expire_at
                        live[2] = 1
                        seg.sets += 1
                        return

        # Brand-new key: reserve one global capacity slot first.
        self._admit(seg)
        with seg.lock:
            live = seg.items.get(key)
            if live is not None and not self._expired(live, time.monotonic()):
                # Another writer won the race after we were admitted; hand
                # the extra permit straight back and perform an in-place set.
                self._release_permit()
                seg.live_bytes += (self._entry_size(key, value)
                                   - self._entry_size(key, live[0]))
                live[0] = value
                live[1] = expire_at
                live[2] = 1
                seg.sets += 1
                return
            # Newly inserted entries start unreferenced: a key never read
            # again is the first CLOCK victim -> approximate LRU behaviour.
            seg.items[key] = [value, expire_at, 0]
            seg.ring.append(key)
            seg.clock_hand = 0
            seg.live_bytes += self._entry_size(key, value)
            seg.sets += 1
            self._evict_if_needed_locked(seg, time.monotonic())

    def _admit(self, seg):
        """Acquire a global permit; evict/expire if the cache is saturated.

        Shard locks are never held while taking another shard's lock and
        never while blocking on the semaphore, so this is deadlock-free.
        """
        while True:
            if self._acquire_permit():
                return
            # Saturated: make room. Prefer the owner shard, then help other
            # shards in a stable order (largest first) to avoid duplicate work.
            now = time.monotonic()
            with seg.lock:
                self._clock_scan_locked(seg, now, len(seg.items) - 1)
            for victim in sorted(self.segments, key=lambda g: len(g.items),
                                 reverse=True):
                if victim is seg:
                    continue
                with victim.lock:
                    self._clock_scan_locked(victim, now, len(victim.items) - 1)
            # Also let reaped TTL entries return permits; then retry.
            time.sleep(0)

    def delete(self, key):
        seg = self._segment(key)
        with seg.lock:
            if key not in seg.items:
                return False
            self._drop_locked(seg, key, "delete")
            seg.deletes += 1
        return True

    def flush(self):
        # Clear shards, then atomically rebuild permit accounting exactly once.
        for seg in self.segments:
            with seg.lock:
                seg.items.clear()
                seg.ring.clear()
                seg.clock_hand = 0
                seg.live_bytes = 0
                seg.sweep_keys = []
        with self._permit_lock:
            self._permits = threading.BoundedSemaphore(self.capacity)
            self._permits_left = self.capacity

    # -- internals: callers must hold the shard lock ----------------------

    def _remove_locked(self, seg, key):
        entry = seg.items.pop(key, None)
        if entry is not None:
            seg.live_bytes -= self._entry_size(key, entry[0])
            if seg.live_bytes < 0:
                seg.live_bytes = 0
        # Returning the permit is done by callers (get/sweeper/_clock_scan)
        # outside data-structure bookkeeping via _drop_locked.

    def _drop_locked(self, seg, key, reason):
        """Remove an item and release its capacity permit. Hold seg.lock."""
        entry = seg.items.pop(key, None)
        if entry is None:
            return False
        seg.live_bytes -= self._entry_size(key, entry[0])
        if seg.live_bytes < 0:
            seg.live_bytes = 0
        if reason == "eviction":
            seg.evictions += 1
        else:
            seg.expirations += 1
        self._release_permit()
        return True

    def _compact_ring_locked(self, seg):
        """Drop deleted/expired tombstones from the CLOCK ring."""
        live = seg.items
        seg.ring = deque(k for k in seg.ring if k in live)
        if seg.clock_hand >= len(seg.ring):
            seg.clock_hand = 0

    def _clock_scan_locked(self, seg, now, target):
        """CLOCK second-chance scan until items <= target.

        Laps 1..2 implement the classic second-chance policy; a third
        forced lap guarantees progress when every ref bit is set. Expired
        entries are reaped (and their permits released) on the way.
        """
        if len(seg.items) <= target:
            return
        if len(seg.ring) > len(seg.items) * 2 + 64:
            self._compact_ring_locked(seg)
        ring = seg.ring
        laps = 0
        while len(seg.items) > target and ring:
            key = ring[seg.clock_hand]
            entry = seg.items.get(key)
            if entry is None:
                pass  # tombstone; removed by periodic compaction
            elif self._expired(entry, now):
                self._drop_locked(seg, key, "expiration")
            elif entry[2] and laps < 2:
                entry[2] = 0
            else:
                self._drop_locked(seg, key, "eviction")
            seg.clock_hand += 1
            if seg.clock_hand >= len(ring):
                seg.clock_hand = 0
                ring = seg.ring
                laps += 1
                if laps >= 3:
                    break
        if len(ring) > len(seg.items) * 2 + 64:
            self._compact_ring_locked(seg)

    def _evict_if_needed_locked(self, seg, now):
        self._clock_scan_locked(seg, now, seg.cap)

    # -- background expiration sweep --------------------------------------

    def sweep_once(self, chunk=256):
        """Expire a bounded slice of each shard; returns entries removed.

        A per-shard cursor persists across calls, so large caches are scanned
        incrementally with short critical sections.
        """
        removed_total = 0
        now = time.monotonic()
        for seg in self.segments:
            with seg.lock:
                if not seg.sweep_keys:
                    seg.sweep_keys = list(seg.items.keys())
                keys = seg.sweep_keys
                batch = keys[:chunk]
                del keys[:chunk]
                removed = 0
                for key in batch:
                    entry = seg.items.get(key)
                    if entry is not None and self._expired(entry, now):
                        self._drop_locked(seg, key, "expiration")
                        removed += 1
            removed_total += removed
        return removed_total

    # -- introspection -----------------------------------------------------

    def sample_keys(self, limit=100):
        now = time.monotonic()
        out = []
        for seg in self.segments:
            with seg.lock:
                for key, entry in seg.items.items():
                    if self._expired(entry, now):
                        continue
                    ttl = entry[1] - now if entry[1] else -1.0
                    out.append((key, entry[0], max(0.0, ttl)))
                    if len(out) >= limit:
                        return out
        return out

    def snapshot(self):
        agg = {
            "items": 0, "capacity": self.capacity,
            "segments": self.segment_count, "bytes": 0,
            "hits": 0, "misses": 0, "sets": 0, "deletes": 0,
            "evictions": 0, "expirations": 0, "seg_stats": [],
            "uptime": time.monotonic() - self.created,
        }
        for i, seg in enumerate(self.segments):
            with seg.lock:
                items = len(seg.items)
                agg["items"] += items
                agg["bytes"] += seg.live_bytes
                agg["hits"] += seg.hits
                agg["misses"] += seg.misses
                agg["sets"] += seg.sets
                agg["deletes"] += seg.deletes
                agg["evictions"] += seg.evictions
                agg["expirations"] += seg.expirations
                agg["seg_stats"].append({
                    "segment": i, "items": items, "soft_capacity": seg.cap,
                    "bytes": seg.live_bytes, "hits": seg.hits,
                    "misses": seg.misses, "evictions": seg.evictions,
                    "expirations": seg.expirations,
                })
        total = agg["hits"] + agg["misses"]
        agg["hit_rate"] = agg["hits"] / total if total else 0.0
        return agg

# ---------------------------------------------------------------------------
# Protocol framing
# ---------------------------------------------------------------------------

MAX_LINE = 64 * 1024
MAX_BUFFER = 256 * 1024
IDLE_TIMEOUT = 30.0


def encode_text_command(line):
    """Parse one raw text-protocol line into a (name, args) tuple."""
    parts = line.split(b" ")
    name = parts[0].upper()
    return name, parts[1:]


def render_stats_text(snap, net):
    lines = [
        "STAT pid %d" % os.getpid(),
        "STAT uptime %.3f" % snap["uptime"],
        "STAT curr_items %d" % snap["items"],
        "STAT total_items %d" % (snap["sets"]),
        "STAT capacity %d" % snap["capacity"],
        "STAT segments %d" % snap["segments"],
        "STAT bytes %d" % snap["bytes"],
        "STAT hits %d" % snap["hits"],
        "STAT misses %d" % snap["misses"],
        "STAT hit_rate %.4f" % snap["hit_rate"],
        "STAT evictions %d" % snap["evictions"],
        "STAT expirations %d" % snap["expirations"],
        "STAT curr_connections %d" % net["curr_connections"],
        "STAT total_connections %d" % net["total_connections"],
        "STAT bytes_read %d" % net["bytes_read"],
        "STAT bytes_written %d" % net["bytes_written"],
        "END",
    ]
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


# ---------------------------------------------------------------------------
# selectors-based dual-protocol reactor
# ---------------------------------------------------------------------------


class Conn:
    __slots__ = ("sock", "addr", "inbuf", "outbuf", "kind", "last_active",
                 "http_ready")

    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.inbuf = b""
        self.outbuf = b""
        self.kind = None           # None = unknown, "text" or "http"
        self.last_active = time.monotonic()
        self.http_ready = b"\r\n\r\n" in self.inbuf


class CacheServer:
    """Single-thread event loop multiplexing HTTP + raw TCP listeners.

    A client connecting to either port may speak either protocol: the first
    request line beginning with an HTTP method is classified as HTTP.
    """

    def __init__(self, cache, host="127.0.0.1", port=7723, http_port=8080):
        self.cache = cache
        self.host = host
        self.port = port
        self.http_port = http_port
        self.selector = selectors.DefaultSelector()
        self.conns = {}
        self.listeners = []
        self.net = {
            "total_connections": 0, "curr_connections": 0,
            "bytes_read": 0, "bytes_written": 0,
        }
        self._closed = threading.Event()
        self._thread = None
        self._started = threading.Event()
        self.shutdown_requested = False
        self._deferred = deque()
        self._deferred_seen = set()

    # -- lifecycle ---------------------------------------------------------

    def _listen(self, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, port))
        srv.listen(128)
        srv.setblocking(False)
        self.selector.register(srv, selectors.EVENT_READ, ("listen", None))
        self.listeners.append(srv)
        return srv.getsockname()[1]

    def start(self):
        self.port = self._listen(self.port)
        if self.http_port is not None and self.http_port >= 0:
            self.http_port = self._listen(self.http_port)
        self._thread = threading.Thread(target=self._run, name="reactor",
                                        daemon=True)
        self._thread.start()
        self._started.wait(2.0)
        return self

    def stop(self, timeout=3.0):
        self._closed.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def join(self):
        if self._thread is not None:
            while self._thread.is_alive():
                self._thread.join(0.5)

    # -- accept / close ----------------------------------------------------

    def _accept(self, srv):
        while True:
            try:
                sock, addr = srv.accept()
            except BlockingIOError:
                return
            sock.setblocking(False)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            conn = Conn(sock, addr)
            self.conns[sock.fileno()] = conn
            self.selector.register(
                sock, selectors.EVENT_READ, ("conn", sock.fileno()))
            self.net["total_connections"] += 1
            self.net["curr_connections"] += 1

    def _close(self, fd):
        conn = self.conns.pop(fd, None)
        if conn is None:
            return
        try:
            self.selector.unregister(conn.sock)
        except (KeyError, ValueError):
            pass
        try:
            conn.sock.close()
        except OSError:
            pass
        self.net["curr_connections"] -= 1

    # -- event loop --------------------------------------------------------

    # Per-tick fairness: total commands drained across all connections and
    # the maximum writes flushed per connection in one loop iteration.
    COMMAND_BUDGET = 2048
    WRITE_BUDGET = 256 * 1024

    def _run(self):
        self._started.set()
        ready = deque()          # round-robin list of connections with data
        seen = set()
        pending_writes = set()

        def enqueue(conn):
            fd = conn.sock.fileno()
            if fd not in seen and fd in self.conns:
                seen.add(fd)
                ready.append(conn)

        while not self._closed.is_set():
            # Accept/IO first so newly connected browsers jump the queue.
            for key, mask in self.selector.select(timeout=1.0):
                tag, fd = key.data
                if tag == "listen":
                    self._accept(key.fileobj)
                    continue
                conn = self.conns.get(fd)
                if conn is None:
                    continue
                try:
                    if mask & selectors.EVENT_READ:
                        self._read_into_buffer(conn)
                        if b"\n" in conn.inbuf or (
                                conn.kind == "http"
                                and b"\r\n\r\n" in conn.inbuf):
                            enqueue(conn)
                    if mask & selectors.EVENT_WRITE:
                        pending_writes.add(fd)
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    self._close(fd)

            # Drain one bounded round across every ready connection. The
            # budget is shared, so N flooding peers each get ~budget/N
            # commands before a fresh select() gives new peers a turn.
            budget = self.COMMAND_BUDGET
            rounds = 0
            while ready and budget > 0 and rounds < len(ready) * 2 + 8:
                conn = ready.popleft()
                seen.discard(conn.sock.fileno())
                if conn.sock.fileno() not in self.conns:
                    continue
                rounds += 1
                try:
                    more = self._process_one_tick(conn, budget)
                    budget = max(0, budget - more[0])
                    if more[1]:
                        enqueue(conn)
                    if conn.outbuf:
                        pending_writes.add(conn.sock.fileno())
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    self._close(conn.sock.fileno())

            # Fair writes too: cap bytes per connection per iteration.
            for fd in list(pending_writes):
                conn = self.conns.get(fd)
                if conn is None:
                    pending_writes.discard(fd)
                    continue
                try:
                    self._flush_writes(conn, self.WRITE_BUDGET)
                    if not conn.outbuf:
                        pending_writes.discard(fd)
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    self._close(fd)
                    pending_writes.discard(fd)

        for fd in list(self.conns):
            self._close(fd)
        for srv in self.listeners:
            try:
                self.selector.unregister(srv)
            except (KeyError, ValueError):
                pass
            srv.close()
        self.selector.close()

    def _read_into_buffer(self, conn):
        try:
            chunk = conn.sock.recv(65536)
        except BlockingIOError:
            return
        if not chunk:
            self._close(conn.sock.fileno())
            return
        conn.last_active = time.monotonic()
        conn.inbuf += chunk
        self.net["bytes_read"] += len(chunk)
        if len(conn.inbuf) > MAX_BUFFER:
            self._close(conn.sock.fileno())
            return
        if conn.kind is None:
            self._classify(conn)
        if time.monotonic() - conn.last_active > IDLE_TIMEOUT:
            self._close(conn.sock.fileno())

    def _process_one_tick(self, conn, budget):
        """Process up to ``budget`` pipelined commands / one HTTP request.

        Returns (commands_handled, more_data_pending).
        """
        if conn.kind == "http":
            if b"\r\n\r\n" in conn.inbuf:
                self._pump_http(conn)
            return (1, False)
        handled = 0
        while budget > handled and b"\n" in conn.inbuf:
            raw, conn.inbuf = conn.inbuf.split(b"\n", 1)
            line = raw.rstrip(b"\r")
            if not line:
                continue
            handled += 1
            if len(line) > MAX_LINE:
                self._queue(conn, b"ERROR line too long\r\n")
                continue
            self._queue(conn, self._handle_text(line))
            if self.shutdown_requested:
                self._closed.set()
        return (handled, b"\n" in conn.inbuf)

    def _flush_writes(self, conn, limit):
        sent_total = 0
        while conn.outbuf and sent_total < limit:
            chunk = conn.outbuf[:max(1, limit - sent_total)]
            try:
                sent = conn.sock.send(chunk)
            except BlockingIOError:
                break
            if sent == 0:
                break
            conn.outbuf = conn.outbuf[sent:]
            sent_total += sent
        self.net["bytes_written"] += sent_total
        if conn.outbuf:
            self._set_write(conn, True)
        else:
            self._set_write(conn, False)
            if conn.kind == "http":
                self._close(conn.sock.fileno())

    def _set_write(self, conn, wanted):
        fd = conn.sock.fileno()
        try:
            cur = self.selector.get_key(conn.sock)
        except KeyError:
            return
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if wanted else 0)
        if cur.events != events:
            self.selector.modify(conn.sock, events, cur.data)

    MAX_OUTBUF = 1024 * 1024

    def _queue(self, conn, payload):
        conn.outbuf += payload
        if len(conn.outbuf) > self.MAX_OUTBUF:
            raise OSError("client too slow: output buffer overflow")

    def _classify(self, conn):
        head = conn.inbuf[:16]
        if head.startswith((b"GET ", b"POST ", b"HEAD ", b"PUT ",
                           b"DELETE ", b"OPTIONS ")):
            conn.kind = "http"
        elif b"\n" in conn.inbuf:
            conn.kind = "text"

    # -- text protocol -----------------------------------------------------

    def _handle_text(self, line):
        name, args = encode_text_command(line)
        try:
            if name == b"PING":
                return b"PONG\r\n"
            if name == b"GET":
                if len(args) != 1:
                    return b"ERROR usage: GET key\r\n"
                key = args[0].decode("utf-8", "replace")
                value = self.cache.get(key)
                if value is None:
                    return b"END\r\n"
                return (b"VALUE " + value.encode("utf-8") + b"\r\nEND\r\n")
            if name == b"SET":
                if len(args) < 3:
                    return b"ERROR usage: SET key seconds value\r\n"
                key = args[0].decode("utf-8", "replace")
                try:
                    ttl = float(args[1])
                except ValueError:
                    return b"ERROR seconds must be a number\r\n"
                value = b" ".join(args[2:]).decode("utf-8", "replace")
                self.cache.set(key, value, ttl)
                return b"STORED\r\n"
            if name == b"DELETE":
                if len(args) != 1:
                    return b"ERROR usage: DELETE key\r\n"
                key = args[0].decode("utf-8", "replace")
                return b"DELETED\r\n" if self.cache.delete(key) else b"NOT_FOUND\r\n"
            if name == b"FLUSH":
                self.cache.flush()
                return b"OK\r\n"
            if name == b"STATS":
                return render_stats_text(self.cache.snapshot(), self.net)
            if name == b"SHUTDOWN":
                self.shutdown_requested = True
                return b"BYE\r\n"
            return b"ERROR unknown command\r\n"
        except Exception as exc:  # protocol errors must never kill a connection
            return ("ERROR internal: %s\r\n" % exc).encode("utf-8", "replace")

    # -- HTTP protocol ------------------------------------------------------

    def _pump_http(self, conn):
        if b"\r\n\r\n" not in conn.inbuf:
            return
        head, _body = conn.inbuf.split(b"\r\n\r\n", 1)
        conn.inbuf = b""  # this prototype only accepts GET; close after reply
        request_line = head.split(b"\r\n", 1)[0]
        try:
            method, target, _version = request_line.split(b" ", 2)
        except ValueError:
            self._queue(conn, _http_response(400, b"Bad Request", "text/plain"))
            return
        path = target.split(b"?", 1)[0].decode("latin-1")
        self._queue(conn, self._route_http(method, path))

    def _route_http(self, method, path):
        if method not in (b"GET", b"HEAD"):
            return _http_response(405, b"Method Not Allowed", "text/plain")
        snap = self.cache.snapshot()
        net = dict(self.net)
        if path == "/" or path == "/dashboard":
            body = render_dashboard(snap, net, self.cache.sample_keys(60),
                                    self.port, self.http_port)
            return _http_response(200, body, "text/html; charset=utf-8")
        if path == "/metrics" or path == "/stats":
            payload = dict(snap)
            payload["network"] = net
            body = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
            return _http_response(200, body, "application/json")
        if path == "/keys":
            keys = [{"key": k, "value": v, "ttl": round(t, 3)}
                    for k, v, t in self.cache.sample_keys(500)]
            body = (json.dumps({"items": keys}, indent=2) + "\n").encode("utf-8")
            return _http_response(200, body, "application/json")
        if path == "/healthz":
            return _http_response(200, b"ok\n", "text/plain")
        if path == "/favicon.ico":
            return _http_response(204, b"", "image/x-icon")
        return _http_response(404, b"not found\n", "text/plain")


def _http_response(status, body, content_type):
    reason = {200: "OK", 204: "No Content", 400: "Bad Request",
              404: "Not Found", 405: "Method Not Allowed"}.get(status, "OK")
    header = (
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "Cache-Control: no-store\r\n"
        "\r\n"
    ) % (status, reason, content_type, len(body))
    return header.encode("latin-1") + body

# ---------------------------------------------------------------------------
# Browser dashboard
# ---------------------------------------------------------------------------


def _human_bytes(n):
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return "%.1f %s" % (n, unit)
        n /= 1024.0


def render_dashboard(snap, net, samples, tcp_port, http_port):
    css = """
    body{font-family:Segoe UI,Helvetica,Arial,sans-serif;margin:0;background:#0f172a;color:#e2e8f0}
    .wrap{max-width:980px;margin:0 auto;padding:24px}
    h1{font-size:22px;margin:0 0 4px} .sub{color:#94a3b8;font-size:13px;margin-bottom:20px}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}
    .card{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:14px 16px}
    .card .k{color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
    .card .v{font-size:24px;font-weight:600;margin-top:4px;font-variant-numeric:tabular-nums}
    h2{font-size:15px;margin:28px 0 8px}
    table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:10px;overflow:hidden}
    th,td{padding:7px 10px;font-size:13px;border-bottom:1px solid #334155;text-align:left}
    th{color:#94a3b8;font-weight:500} tr:last-child td{border-bottom:none}
    code{color:#7dd3fc} .bar{height:6px;background:#334155;border-radius:4px;overflow:hidden}
    .bar i{display:block;height:100%;background:#38bdf8}
    a{color:#7dd3fc}
    """
    occ = snap["items"] / snap["capacity"] if snap["capacity"] else 0
    rows = []
    for seg in snap["seg_stats"]:
        fill = int(100 * seg["items"] / seg["soft_capacity"]) if seg["soft_capacity"] else 0
        rows.append(
            "<tr><td>%d</td><td>%d / %d</td><td>%s</td><td>%d</td><td>%d</td>"
            '<td><div class="bar"><i style="width:%d%%"></i></div></td></tr>'
            % (seg["segment"], seg["items"], seg["soft_capacity"],
               _human_bytes(seg["bytes"]), seg["hits"], seg["evictions"], fill))
    key_rows = []
    for key, value, ttl in samples:
        shown = value if len(value) <= 80 else value[:77] + "..."
        ttl_text = "forever" if ttl < 0 else "%.1fs" % ttl
        key_rows.append("<tr><td><code>%s</code></td><td>%s</td><td>%s</td></tr>"
                        % (html.escape(key), html.escape(shown), ttl_text))
    if not key_rows:
        key_rows.append('<tr><td colspan="3">cache is empty</td></tr>')

    doc = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2">
<title>mcached</title><style>{css}</style></head>
<body><div class="wrap">
<h1>mcached</h1>
<div class="sub">dual-protocol multiplexed cache &middot; auto-refresh 2s
&middot; tcp <code>{tcp}</code> http <code>{http}</code>
&middot; <a href="/metrics">/metrics JSON</a> &middot; <a href="/keys">/keys</a></div>
<div class="grid">
{cards}
</div>
<h2>Capacity &middot; {occ:.1%} used</h2>
<div class="bar" style="height:10px"><i style="width:{occpct}%"></i></div>
<h2>Shard segments (segmented locking)</h2>
<table><thead><tr><th>#</th><th>items / cap</th><th>memory</th><th>hits</th>
<th>evictions</th><th>fill</th></tr></thead><tbody>{segrows}</tbody></table>
<h2>Key sample</h2>
<table><thead><tr><th>key</th><th>value</th><th>ttl</th></tr></thead>
<tbody>{keyrows}</tbody></table>
</div></body></html>"""
    cards = [
        ("Items", "{:,} / {:,}".format(snap["items"], snap["capacity"])),
        ("Memory", _human_bytes(snap["bytes"])),
        ("Hit rate", "%.2f%%" % (snap["hit_rate"] * 100.0)),
        ("Hits / Misses", "{:,} / {:,}".format(snap["hits"], snap["misses"])),
        ("Sets", "{:,}".format(snap["sets"])),
        ("Evictions", "{:,}".format(snap["evictions"])),
        ("Expirations", "{:,}".format(snap["expirations"])),
        ("Connections", "{:,} active / {:,} total".format(
            net["curr_connections"], net["total_connections"])),
        ("Bytes in / out", "{0} / {1}".format(
            _human_bytes(net["bytes_read"]), _human_bytes(net["bytes_written"]))),
        ("Uptime", "%.1fs" % snap["uptime"]),
    ]
    cards_html = "".join(
        '<div class="card"><div class="k">%s</div><div class="v">%s</div></div>'
        % (k, v) for k, v in cards)
    return doc.format(
        css=css, cards=cards_html, occ=occ, occpct=int(occ * 100),
        segrows="".join(rows), keyrows="".join(key_rows),
        tcp=tcp_port, http=http_port).encode("utf-8")

# ---------------------------------------------------------------------------
# Background stress workload
# ---------------------------------------------------------------------------


class StressWorkers:
    """Random SET/GET/DELETE traffic with a mix of expiring keys."""

    def __init__(self, cache, keyspace=4000, workers=4, stop_event=None):
        self.cache = cache
        self.keyspace = keyspace
        self.workers = workers
        self.stop_event = stop_event or threading.Event()
        self.threads = []
        self.ops = 0
        self._counter_lock = threading.Lock()

    def _worker(self):
        local = random.Random()
        done = 0
        while not self.stop_event.is_set():
            key = "k%d" % local.randrange(self.keyspace)
            roll = local.random()
            if roll < 0.45:
                ttl = local.choice([0, 0, 0, 0.15, 1.0, 5.0])
                self.cache.set(key, local.getrandbits(48).__str__(), ttl)
            elif roll < 0.9:
                self.cache.get(key)
            else:
                self.cache.delete(key)
            done += 1
            if done & 1023 == 0:
                with self._counter_lock:
                    self.ops += done
                done = 0
        with self._counter_lock:
            self.ops += done

    def start(self):
        for i in range(self.workers):
            t = threading.Thread(target=self._worker, name="stress-%d" % i,
                                 daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=2.0)


def _sweeper_loop(cache, stop_event, interval=0.2, chunk=256):
    while not stop_event.wait(interval):
        try:
            cache.sweep_once(chunk)
        except Exception:
            pass


def _monitor_loop(cache, net_getter, stop_event, interval=3.0):
    last = None
    while not stop_event.wait(interval):
        snap = cache.snapshot()
        now = snap["uptime"]
        sets, hits, misses = snap["sets"], snap["hits"], snap["misses"]
        if last is not None:
            dt = now - last[0]
            rate = (sets + hits + misses - last[1] - last[2] - last[3]) / max(dt, 1e-9)
            rate_text = ", ops/s ~%.0f" % rate
        else:
            rate_text = ""
        last = (now, sets, hits, misses)
        print("[monitor] items=%d/%d mem=%s hits=%d misses=%d evict=%d expire=%d%s"
              % (snap["items"], snap["capacity"], _human_bytes(snap["bytes"]),
                 hits, misses, snap["evictions"], snap["expirations"], rate_text),
              flush=True)
        net = net_getter()
        print("           conns=%d/%d net=%s/%s"
              % (net["curr_connections"], net["total_connections"],
                 _human_bytes(net["bytes_read"]),
                 _human_bytes(net["bytes_written"])), flush=True)


def serve_forever(args):
    cache = SegmentedCache(capacity=args.capacity, segments=args.segments)
    server = CacheServer(cache, host=args.host, port=args.port,
                         http_port=args.http_port).start()
    stop_event = threading.Event()
    bg = []
    sweeper = threading.Thread(target=_sweeper_loop,
                               args=(cache, stop_event), name="sweeper",
                               daemon=True)
    sweeper.start()
    bg.append(sweeper)
    stress = None
    if not args.no_stress:
        stress = StressWorkers(cache, keyspace=args.keyspace,
                               workers=args.stress_workers)
        stress.start()
    monitor = threading.Thread(
        target=_monitor_loop,
        args=(cache, lambda: server.net, stop_event),
        name="monitor", daemon=True)
    monitor.start()
    bg.append(monitor)

    print("mcached listening on http://%s:%d  (text+http auto-detect)"
          % (args.host, server.port), flush=True)
    if args.http_port:
        print("         dedicated http http://%s:%d"
              % (args.host, server.http_port), flush=True)
    print("text protocol:  printf 'PING\\r\\n' | nc %s %d"
          % (args.host, server.port), flush=True)

    duration = args.duration if args.duration > 0 else None
    try:
        if duration is not None:
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline and not server.shutdown_requested:
                time.sleep(0.1)
        else:
            while not server.shutdown_requested:
                time.sleep(0.2)
    except KeyboardInterrupt:
        print("\nshutting down ...", flush=True)
    finally:
        stop_event.set()
        if stress is not None:
            stress.stop()
        server.stop()
    return 0

# ---------------------------------------------------------------------------
# Built-in concurrency / consistency tests
# ---------------------------------------------------------------------------


class CacheBasicTests(unittest.TestCase):
    def test_set_get_delete(self):
        c = SegmentedCache(capacity=10, segments=3)
        self.assertIsNone(c.get("a"))
        c.set("a", "1")
        self.assertEqual(c.get("a"), "1")
        self.assertTrue(c.delete("a"))
        self.assertIsNone(c.get("a"))
        self.assertFalse(c.delete("a"))

    def test_update_in_place(self):
        c = SegmentedCache(capacity=10, segments=2)
        c.set("a", "x" * 100)
        c.set("a", "y" * 5)
        self.assertEqual(c.get("a"), "y" * 5)
        self.assertEqual(c.snapshot()["items"], 1)

    def test_flush(self):
        c = SegmentedCache(capacity=10)
        for i in range(5):
            c.set("k%d" % i, "v")
        c.flush()
        snap = c.snapshot()
        self.assertEqual(snap["items"], 0)
        self.assertEqual(snap["bytes"], 0)

    def test_hit_miss_counters(self):
        c = SegmentedCache(capacity=10, segments=4)
        c.set("a", "1")
        c.get("a")
        c.get("a")
        c.get("missing")
        snap = c.snapshot()
        self.assertEqual(snap["hits"], 2)
        self.assertEqual(snap["misses"], 1)
        self.assertAlmostEqual(snap["hit_rate"], 2 / 3)

    def test_global_capacity_matches_request(self):
        c = SegmentedCache(capacity=10, segments=3)
        self.assertEqual(c.capacity, 10)
        self.assertEqual(c.segment_count, 3)
        for seg in c.segments:
            self.assertGreaterEqual(seg.cap, 1)


class TTLTests(unittest.TestCase):
    def test_lazy_expiration(self):
        c = SegmentedCache(capacity=10)
        c.set("soon", "v", ttl=0.05)
        self.assertEqual(c.get("soon"), "v")
        time.sleep(0.08)
        self.assertIsNone(c.get("soon"))
        self.assertEqual(c.snapshot()["items"], 0)

    def test_background_sweeper(self):
        c = SegmentedCache(capacity=200, segments=4)
        for i in range(60):
            c.set("e%d" % i, "v", ttl=0.02)
        for i in range(20):
            c.set("p%d" % i, "v", ttl=0)
        time.sleep(0.05)
        for _ in range(10):
            c.sweep_once(chunk=32)
        snap = c.snapshot()
        self.assertEqual(snap["items"], 20)
        self.assertGreaterEqual(snap["expirations"], 60)

    def test_ttl_zero_is_forever(self):
        c = SegmentedCache(capacity=10)
        c.set("keep", "v", ttl=0)
        time.sleep(0.03)
        self.assertEqual(c.get("keep"), "v")

    def test_resurrect_after_expiry(self):
        c = SegmentedCache(capacity=10)
        c.set("k", "old", ttl=0.02)
        time.sleep(0.04)
        c.set("k", "new", ttl=0)
        self.assertEqual(c.get("k"), "new")
        self.assertEqual(c.snapshot()["items"], 1)


class EvictionTests(unittest.TestCase):
    def test_never_exceeds_capacity(self):
        c = SegmentedCache(capacity=50, segments=4)
        for i in range(500):
            c.set("k%d" % i, "v%d" % i)
        snap = c.snapshot()
        self.assertLessEqual(snap["items"], 50)
        for seg in c.segments:
            self.assertLessEqual(len(seg.items), seg.cap)
        self.assertGreater(snap["evictions"], 0)

    def test_clock_approximates_lru(self):
        c = SegmentedCache(capacity=3, segments=1)
        c.set("a", "1")
        c.set("b", "2")
        c.set("c", "3")
        for _ in range(5):
            self.assertEqual(c.get("a"), "1")
        c.set("d", "4")
        self.assertIsNone(c.get("b"))       # least recently used is reclaimed
        self.assertEqual(c.get("a"), "1")   # hot key survives
        self.assertEqual(c.get("c"), "3")
        self.assertEqual(c.get("d"), "4")

    def test_memory_accounting_after_eviction_and_delete(self):
        c = SegmentedCache(capacity=20, segments=2)
        for i in range(100):
            c.set("k%d" % i, "x" * 50)
        for i in range(0, 100, 3):
            c.delete("k%d" % i)
        counted = 0
        for seg in c.segments:
            with seg.lock:
                counted += sum(SegmentedCache._entry_size(k, e[0])
                               for k, e in seg.items.items())
        self.assertEqual(c.snapshot()["bytes"], counted)

    def test_ring_compaction_no_unbounded_growth(self):
        c = SegmentedCache(capacity=10, segments=1)
        for i in range(2000):
            key = "k%d" % (i % 15)
            c.set(key, "v")
            if i % 5 == 0:
                c.delete(key)
        seg = c.segments[0]
        self.assertLessEqual(len(seg.ring), len(seg.items) * 2 + 64)


class ConcurrencyTests(unittest.TestCase):
    def test_parallel_unique_writes_readable(self):
        c = SegmentedCache(capacity=20000, segments=8)
        n_threads, per = 8, 500

        def writer(tid):
            for i in range(per):
                key = "t%d-k%d" % (tid, i)
                c.set(key, "v%d-%d" % (tid, i))

        threads = [threading.Thread(target=writer, args=(t,))
                   for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        expected = n_threads * per
        self.assertEqual(c.snapshot()["items"], expected)
        misses = [0]

        def reader(tid):
            for i in range(per):
                if c.get("t%d-k%d" % (tid, i)) != "v%d-%d" % (tid, i):
                    misses[0] += 1

        threads = [threading.Thread(target=reader, args=(t,))
                   for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(misses[0], 0)

    def test_capacity_invariant_under_contention(self):
        c = SegmentedCache(capacity=500, segments=8)
        stop = threading.Event()

        def worker(tid):
            rnd = random.Random(tid)
            i = 0
            while not stop.is_set():
                key = "w%d-%d" % (tid, rnd.randrange(3000))
                c.set(key, str(i), ttl=rnd.choice([0, 0, 0.01, 0.05]))
                if i % 3 == 0:
                    c.get(key)
                elif i % 7 == 0:
                    c.delete(key)
                i += 1

        threads = [threading.Thread(target=worker, args=(t,))
                   for t in range(8)]
        for t in threads:
            t.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            snap = c.snapshot()
            self.assertLessEqual(snap["items"], 500)
            for seg in c.segments:
                self.assertLessEqual(len(seg.items), seg.cap)
            time.sleep(0.01)
        stop.set()
        for t in threads:
            t.join(timeout=3)

    def test_sweeper_concurrent_with_traffic(self):
        c = SegmentedCache(capacity=300, segments=4)
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set():
                c.set("x%d" % (i % 800), "v", ttl=0.02)
                i += 1

        threads = [threading.Thread(target=writer) for _ in range(4)]
        for t in threads:
            t.start()
        for _ in range(40):
            c.sweep_once(chunk=64)
            time.sleep(0.01)
        stop.set()
        for t in threads:
            t.join(timeout=2)
        for seg in c.segments:
            self.assertLessEqual(len(seg.items), seg.cap)
        self.assertEqual(c.snapshot()["bytes"] >= 0, True)


class ProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache = SegmentedCache(capacity=500, segments=4)
        cls.server = CacheServer(cls.cache, "127.0.0.1", port=0,
                                 http_port=0).start()
        cls.tcp = cls.server.port
        cls.http = cls.server.http_port

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def _text(self, port, commands):
        with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
            s.sendall(commands)
            # Text connections are keep-alive; drain replies until a short
            # idle gap signals that all pipelined responses have arrived.
            s.settimeout(0.6)
            data = b""
            while True:
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                data += chunk
            return data

    def test_text_protocol_roundtrip(self):
        data = self._text(self.tcp, (
            b"PING\r\nSET foo 0 bar\r\nGET foo\r\nDELETE foo\r\n"
            b"GET foo\r\nBOGUS\r\nSTATS\r\n"))
        self.assertIn(b"PONG", data)
        self.assertIn(b"STORED", data)
        self.assertIn(b"VALUE bar", data)
        self.assertIn(b"DELETED", data)
        self.assertIn(b"END\r\n", data)
        self.assertIn(b"ERROR unknown command", data)
        self.assertIn(b"STAT capacity 500", data)

    def test_text_protocol_autodetect_on_http_port(self):
        data = self._text(self.http, b"PING\r\n")
        self.assertIn(b"PONG", data)

    def test_http_metrics_and_dashboard(self):
        with socket.create_connection(("127.0.0.1", self.tcp), timeout=3) as s:
            s.sendall(b"SET httpkey 0 httpval\r\n")
            self.assertIn(b"STORED", s.recv(1024))

        def http_get(port, path):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
                s.sendall(("GET %s HTTP/1.0\r\nHost: x\r\n\r\n" % path).encode())
                s.settimeout(3)
                buf = b""
                while True:
                    chunk = s.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
            return buf

        for port in (self.tcp, self.http):
            resp = http_get(port, "/healthz")
            self.assertTrue(resp.startswith(b"HTTP/1.1 200"))
            self.assertIn(b"ok", resp)

        resp = http_get(self.http, "/metrics")
        self.assertIn(b"HTTP/1.1 200", resp)
        body = resp.split(b"\r\n\r\n", 1)[1]
        metrics = json.loads(body.decode())
        self.assertEqual(metrics["capacity"], 500)
        self.assertIn("network", metrics)
        self.assertGreaterEqual(metrics["items"], 1)

        resp = http_get(self.tcp, "/keys")
        body = resp.split(b"\r\n\r\n", 1)[1]
        keys = json.loads(body.decode())["items"]
        self.assertTrue(any(k["key"] == "httpkey" for k in keys))

        resp = http_get(self.http, "/")
        self.assertIn(b"text/html", resp)
        self.assertIn(b"mcached", resp)

        resp = http_get(self.http, "/nope")
        self.assertTrue(resp.startswith(b"HTTP/1.1 404"))


def run_selftest():
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def build_parser():
    p = argparse.ArgumentParser(description="Dual-protocol multiplexed cache")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=7723,
                   help="main port, text+http auto-detected (0 = ephemeral)")
    p.add_argument("--http-port", type=int, default=8080,
                   help="dedicated HTTP port, 0 = ephemeral, -1 = disabled")
    p.add_argument("--capacity", type=int, default=100000)
    p.add_argument("--segments", type=int, default=16)
    p.add_argument("--keyspace", type=int, default=4000)
    p.add_argument("--stress-workers", type=int, default=4)
    p.add_argument("--no-stress", action="store_true")
    p.add_argument("--duration", type=float, default=0,
                   help="auto-stop after N seconds (0 = run forever)")
    p.add_argument("--selftest", action="store_true")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.selftest:
        return run_selftest()
    if args.http_port == -1:
        args.http_port = None
    return serve_forever(args)


if __name__ == "__main__":
    sys.exit(main())
