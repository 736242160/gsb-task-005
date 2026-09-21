import json, subprocess, sys, time, urllib.request, socket, os

ROOT = os.path.dirname(os.path.abspath(__file__))
proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "mcached.py"),
     "--port", "17723", "--http-port", "18080",
     "--capacity", "8000", "--segments", "8", "--duration", "10"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
try:
    time.sleep(4)
    s = socket.create_connection(("127.0.0.1", 17723), timeout=3)
    s.sendall(b"PING\r\nSET smoke 0 hello\r\nGET smoke\r\nSTATS\r\n")
    s.settimeout(2)
    data = b""
    try:
        while True:
            chunk = s.recv(8192)
            if not chunk:
                break
            data += chunk
            if b"END\r\n" in data and data.count(b"END\r\n") >= 2:
                break
    except socket.timeout:
        pass
    s.close()
    print("=== TCP REPLY ===")
    print(data.decode())

    metrics = json.load(urllib.request.urlopen(
        "http://127.0.0.1:18080/metrics", timeout=3))
    print("=== METRICS (http port) ===")
    for k in ("items", "capacity", "hit_rate", "evictions", "expirations",
              "bytes", "segments"):
        print(" ", k, "=", metrics[k])
    print("  network =", metrics["network"])
    assert metrics["items"] <= metrics["capacity"]
    assert metrics["segments"] == 8

    resp = urllib.request.urlopen("http://127.0.0.1:17723/", timeout=3)
    body = resp.read().decode()
    print("=== DASHBOARD (main port) ===", resp.status,
          resp.headers["Content-Type"], len(body), "bytes")
    assert resp.status == 200 and "mcached" in body

    keys = json.load(urllib.request.urlopen(
        "http://127.0.0.1:18080/keys", timeout=3))
    print("  /keys sample size:", len(keys["items"]))
    print("SMOKE OK")
finally:
    try:
        out, _ = proc.communicate(timeout=12)
        print("=== SERVER LOG (tail) ===")
        print("\n".join(out.splitlines()[-8:]))
    except subprocess.TimeoutExpired:
        proc.kill()