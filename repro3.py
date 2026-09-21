import subprocess, sys, time, socket, os, threading, json, urllib.request
ROOT = os.path.dirname(os.path.abspath(__file__))
proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "mcached.py"),
     "--port", "17733", "--http-port", "18093",
     "--capacity", "5000", "--segments", "8", "--no-stress"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    time.sleep(1.0)

    # 1) A deliberately slow client: sends SETs but never reads replies.
    slow = socket.create_connection(("127.0.0.1", 17733), timeout=5)
    for i in range(50000):
        try:
            slow.sendall(("SET slow%d 0 v\r\n" % i).encode())
        except (BlockingIOError, OSError):
            break
    print("slow client pushed until backpressure, server still alive")

    # 2) Meanwhile, other clients must keep working with low latency.
    for _ in range(5):
        c = socket.create_connection(("127.0.0.1", 17733), timeout=3)
        c.settimeout(3)
        c.sendall(b"PING\r\n")
        assert c.recv(64) == b"PONG\r\n"
        c.close()
    m = json.load(urllib.request.urlopen("http://127.0.0.1:18093/metrics", timeout=3))
    print("during slow client: items=%d cap=%d active_conns=%d"
          % (m["items"], m["capacity"], m["network"]["curr_connections"]))
    assert m["items"] <= m["capacity"]
    slow.close()

    # 3) Long saturated run: capacity invariant must always hold (permit leak
    #    would either overshoot or permanently wedge inserts below capacity).
    stop = threading.Event()
    def loader(tid):
        import random
        r = random.Random(100 + tid)
        c = socket.create_connection(("127.0.0.1", 17733), timeout=5)
        c.setblocking(False)
        i = 0
        while not stop.is_set():
            cmd = ("SET x%d-%d 0 z\r\n" % (tid, r.randrange(20000))).encode()
            try:
                c.sendall(cmd)
            except OSError:
                break
            i += 1
            if i % 128 == 0:
                try:
                    while True:
                        d = c.recv(65536)
                        if not d: raise OSError("closed")
                except BlockingIOError:
                    pass
        c.close()
    ts = [threading.Thread(target=loader, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    over = 0
    min_items = None
    for _ in range(60):
        m = json.load(urllib.request.urlopen("http://127.0.0.1:18093/metrics", timeout=3))
        if m["items"] > m["capacity"]:
            over += 1
        min_items = m["items"] if min_items is None else min(min_items, m["items"])
        time.sleep(0.1)
    stop.set()
    for t in ts: t.join(timeout=5)
    m = json.load(urllib.request.urlopen("http://127.0.0.1:18093/metrics", timeout=3))
    print("after 6s saturated run: items=%d evictions=%d sets=%d overshoots=%d"
          % (m["items"], m["evictions"], m["sets"], over))
    assert over == 0 and m["items"] == m["capacity"], (over, m["items"])
    print("SLOW-CLIENT + PERMIT-LEAK CHECK OK")
finally:
    proc.terminate()
    try: proc.wait(timeout=5)
    except Exception: proc.kill()