import subprocess, sys, time, socket, os
ROOT = os.path.dirname(os.path.abspath(__file__))
proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "mcached.py"),
     "--port", "17731", "--http-port", "18091",
     "--capacity", "5000", "--no-stress"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(1.5)
try:
    s = socket.create_connection(("127.0.0.1", 17731), timeout=3)
    s.settimeout(3)
    s.sendall(b"PING\r\n")
    print("reply:", s.recv(1024))
    s.close()
    s = socket.create_connection(("127.0.0.1", 17731), timeout=3)
    s.settimeout(3)
    s.sendall(b"SET a 0 1\r\nGET a\r\n")
    time.sleep(0.3)
    print("reply2:", s.recv(4096))
    s.close()
finally:
    proc.terminate()
    out,_ = proc.communicate(timeout=5)
    print(out[-500:])