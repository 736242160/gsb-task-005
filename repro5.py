import subprocess, sys, time, socket, os, threading, json, urllib.request
ROOT=os.path.dirname(os.path.abspath(__file__))
proc=subprocess.Popen([sys.executable,os.path.join(ROOT,"mcached.py"),
 "--port","17735","--http-port","18095","--capacity","20000","--segments","16","--no-stress"],
 stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
stop=threading.Event()
stats={"sets":0,"gets":0}
lock=threading.Lock()
def loader(tid):
    import random
    r=random.Random(300+tid)
    try:
        c=socket.create_connection(("127.0.0.1",17735),timeout=5)
        c.setblocking(False)
        out=bytearray(); done=0
        local_sets=local_gets=0
        while not stop.is_set():
            for _ in range(128):
                k=r.randrange(60000)
                if r.random()<0.7:
                    out += ("SET z%d-%d 0 v\r\n"%(tid,k)).encode(); local_sets+=1
                else:
                    out += ("GET z%d-%d\r\n"%(tid,k)).encode(); local_gets+=1
            # write what we can
            try:
                n=c.send(bytes(out))
                del out[:n]
            except BlockingIOError:
                pass
            # read replies to release backpressure
            try:
                while True:
                    d=c.recv(65536)
                    if not d: raise OSError
            except BlockingIOError:
                pass
            if len(out) > 2_000_000:
                time.sleep(0.005)
        with lock:
            stats["sets"]+=local_sets; stats["gets"]+=local_gets
        c.close()
    except OSError:
        pass
try:
    time.sleep(1.0)
    ts=[threading.Thread(target=loader,args=(i,)) for i in range(12)]
    for t in ts:t.start()
    lat=[]
    over=0
    t_end=time.time()+5
    while time.time()<t_end:
        t0=time.time()
        m=json.load(urllib.request.urlopen("http://127.0.0.1:18095/metrics",timeout=3))
        lat.append(time.time()-t0)
        if m["items"]>m["capacity"]: over+=1
        time.sleep(0.1)
    stop.set()
    for t in ts:t.join(timeout=5)
    m=json.load(urllib.request.urlopen("http://127.0.0.1:18095/metrics",timeout=3))
    print("items=%d/%d evict=%d exp=%d overshoot=%d"%(
        m["items"],m["capacity"],m["evictions"],m["expirations"],over))
    print("server sets=%d hits=%d misses=%d"%(m["sets"],m["hits"],m["misses"]))
    print("http latency avg=%.1fms max=%.1fms"%(1000*sum(lat)/len(lat),1000*max(lat)))
    print("active conns:",m["network"]["curr_connections"])
    assert over==0 and m["items"]<=m["capacity"]
    assert max(lat)<0.5, "HTTP latency regression"
    print("THROUGHPUT/FAIRNESS OK")
finally:
    proc.terminate()
    try:proc.wait(timeout=5)
    except Exception:proc.kill()