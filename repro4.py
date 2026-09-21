import subprocess, sys, time, socket, os, threading, json, urllib.request
ROOT = os.path.dirname(os.path.abspath(__file__))
proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "mcached.py"),
     "--port", "17734", "--http-port", "18094",
     "--capacity", "5000", "--segments", "8", "--no-stress"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
try:
    time.sleep(1.0)
    stop = threading.Event()
    def loader(tid):
        import random
        r = random.Random(200 + tid)
        try:
            c = socket.create_connection(("127.0.0.1", 17734), timeout=5)
            c.setblocking(False)
            i = 0
            while not stop.is_set():
                cmd = ("SET y%d-%d 0 z\r\n" % (tid, r.randrange(20000))).encode()
                c.sendall(cmd)
                i += 1
                if i % 64 == 0:
                    time.sleep(0.002)
                    try:
                        while True:
                            d = c.recv(65536)
                            if not d: raise OSError
                    except BlockingIOError:
                        pass
        except OSError:
            pass
    ts=[threading.Thread(target=loader,args=(i,)) for i in range(8)]
    for t in ts: t.start()
    time.sleep(2)
    t0=time.time()
    resp=urllib.request.urlopen("http://127.0.0.1:18094/healthz",timeout=8)
    print("healthz", resp.status, "latency=%.2fs" % (time.time()-t0))
    t0=time.time()
    m=json.load(urllib.request.urlopen("http://127.0.0.1:18094/metrics",timeout=8))
    print("metrics ok latency=%.2fs items=%d sets=%d conns=%s" % (
        time.time()-t0, m["items"], m["sets"], m["network"]))
    stop.set()
    for t in ts: t.join(timeout=5)
finally:
    proc.terminate()
    out,_=proc.communicate(timeout=5)
    print("LOG TAIL:\n"+ "\n".join(out.splitlines()[-10:]))