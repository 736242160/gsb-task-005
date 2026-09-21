import json, subprocess, sys, time, urllib.request, os

ROOT = os.path.dirname(os.path.abspath(__file__))
proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "mcached.py"),
     "--port", "17724", "--http-port", "18081",
     "--capacity", "5000", "--segments", "8",
     "--keyspace", "30000", "--stress-workers", "8", "--duration", "8",
     "--no-stress"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# custom loaders: all permanent keys, force evictions + saturation
import threading, random, socket
stop = threading.Event()
cache_port = 17724

def loader(tid):
    r = random.Random(tid)
    s = socket.create_connection(("127.0.0.1", cache_port), timeout=5)
    i = 0
    while not stop.is_set():
        key = "w%d-%d" % (tid, r.randrange(30000))
        if i % 4 == 0:
            cmd = ("GET %s\r\n" % key).encode()
            s.sendall(cmd)
            s.recv(4096)
        else:
            cmd = ("SET %s 0 v%d\r\n" % (key, i)).encode()
            s.sendall(cmd)
            s.recv(64)
        i += 1
    s.close()

try:
    time.sleep(1.0)
    ts = [threading.Thread(target=loader, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    over = []
    for _ in range(30):
        m = json.load(urllib.request.urlopen(
            "http://127.0.0.1:18081/metrics", timeout=3))
        if m["items"] > m["capacity"]:
            over.append((m["items"], m["capacity"]))
        time.sleep(0.1)
    stop.set()
    for t in ts: t.join(timeout=3)
    m = json.load(urllib.request.urlopen(
        "http://127.0.0.1:18081/metrics", timeout=3))
    print("final items=%d capacity=%d evictions=%d hits=%d misses=%d"
          % (m["items"], m["capacity"], m["evictions"], m["hits"], m["misses"]))
    print("overshoot samples:", over[:5], "count:", len(over))
    assert not over, "CAPACITY OVERSHOOT DETECTED"
    assert m["evictions"] > 0, "eviction path never exercised"
    assert m["items"] <= m["capacity"]
    print("EVICTION/SATURATION OK")
finally:
    proc.terminate()
    try: proc.wait(timeout=5)
    except Exception: proc.kill()