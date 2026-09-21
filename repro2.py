import subprocess, sys, time, socket, os, threading, json, urllib.request
ROOT = os.path.dirname(os.path.abspath(__file__))
proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "mcached.py"),
     "--port", "17732", "--http-port", "18092",
     "--capacity", "5000", "--segments", "8", "--no-stress"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
stop = threading.Event()
errs = []
def loader(tid):
    import random
    r = random.Random(tid)
    try:
        s = socket.create_connection(("127.0.0.1", 17732), timeout=5)
        s.setblocking(False)
        pending = 0
        i = 0
        while not stop.is_set():
            key = "w%d-%d" % (tid, r.randrange(30000))
            cmd = ("SET %s 0 v%d\r\n" % (key, i)).encode()
            s.sendall(cmd)
            pending += 1
            i += 1
            # drain every N ops to keep receive buffer drained
            if i % 32 == 0:
                time.sleep(0.002)
                try:
                    while True:
                        d = s.recv(65536)
                        if not d:
                            raise EOFError("server closed")
                        pending -= d.count(b"\r\n")
                except BlockingIOError:
                    pass
                if pending > 4096:
                    errs.append(("backlog", tid, pending))
                    return
        # final drain
        s.settimeout(2)
        s.setblocking(True)
        try:
            while True:
                d = s.recv(65536)
                if not d: break
        except socket.timeout:
            pass
        s.close()
    except Exception as e:
        errs.append((repr(e), tid))
try:
    time.sleep(1.0)
    ts=[threading.Thread(target=loader,args=(i,)) for i in range(8)]
    for t in ts: t.start()
    time.sleep(4)
    stop.set()
    for t in ts: t.join(timeout=6)
    m=json.load(urllib.request.urlopen("http://127.0.0.1:18092/metrics",timeout=3))
    print("items=%d cap=%d evictions=%d sets=%d net=%s" % (
        m["items"], m["capacity"], m["evictions"], m["sets"], m["network"]))
    print("errors:", errs[:5], "count", len(errs))
    assert not errs
    assert m["items"] <= m["capacity"]
    assert m["evictions"] > 0
    print("HIGH-CONN EVICTION OK")
finally:
    proc.terminate()
    try: proc.wait(timeout=5)
    except Exception: proc.kill()