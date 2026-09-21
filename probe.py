import subprocess, sys, time, socket, os, threading, json, urllib.request
ROOT=os.path.dirname(os.path.abspath(__file__))
proc=subprocess.Popen([sys.executable,os.path.join(ROOT,"mcached.py"),
 "--port","17736","--http-port","18096","--capacity","20000","--segments","16","--no-stress"],
 stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
stop=threading.Event()
def loader(tid):
    import random
    r=random.Random(400+tid)
    try:
        c=socket.create_connection(("127.0.0.1",17736),timeout=5)
        c.setblocking(False)
        out=bytearray()
        while not stop.is_set():
            for _ in range(128):
                out += ("SET q%d-%d 0 v\r\n"%(tid,r.randrange(60000))).encode()
            try:
                n=c.send(bytes(out)); del out[:n]
            except BlockingIOError: pass
            try:
                while True:
                    d=c.recv(65536)
                    if not d: raise OSError
            except BlockingIOError: pass
            if len(out)>2_000_000: time.sleep(0.005)
        c.close()
    except OSError: pass
try:
    time.sleep(1.0)
    ts=[threading.Thread(target=loader,args=(i,)) for i in range(12)]
    for t in ts:t.start()
    time.sleep(2)
    # raw socket PING latency on the MAIN port (flooded) vs dedicated HTTP
    for name,port in [("main","17736"),("http","18096")]:
        t0=time.time()
        try:
            c=socket.create_connection(("127.0.0.1",int(port)),timeout=5)
            c.settimeout(5)
            c.sendall(b"PING\r\n")
            r=c.recv(64); print(name,"PING latency=%.3fs reply=%r"%(time.time()-t0,r))
            c.close()
        except Exception as e:
            print(name,"failed after %.2fs:"%(time.time()-t0),repr(e))
    # HTTP request latency
    t0=time.time()
    try:
        r=urllib.request.urlopen("http://127.0.0.1:18096/healthz",timeout=5)
        print("http GET latency=%.3fs"%(time.time()-t0))
    except Exception as e:
        print("http GET failed after %.2fs"%(time.time()-t0),repr(e))
finally:
    stop.set()
    for t in ts:t.join(timeout=3)
    proc.terminate()
    try:proc.wait(timeout=5)
    except Exception:proc.kill()