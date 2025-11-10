import sys, requests, json
q = " ".join(sys.argv[1:]) or "Thứ 4 (20/8) có hoạt động gì? ở đâu?"
r = requests.post("http://127.0.0.1:8000/ask", json={"question": q}, timeout=60)
print(json.dumps(r.json(), ensure_ascii=False, indent=2))
