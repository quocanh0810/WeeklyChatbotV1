import argparse, os, json, sqlite3, hashlib
import numpy as np, faiss
from sentence_transformers import SentenceTransformer

def chunk_text_fields(ev):
    fields = []
    for k in ("date","dow","start","end","location","participants","title"):
        if ev.get(k):
            fields.append(f"{k}: {ev[k]}")
    if ev.get("raw"):
        fields.append(f"raw: {ev['raw']}")
    return "\n".join(fields)

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chunks(
        id INTEGER PRIMARY KEY,
        hash TEXT UNIQUE,
        text TEXT,
        date TEXT, dow TEXT, start TEXT, end TEXT,
        location TEXT, participants TEXT, title TEXT, raw TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta(
        k TEXT PRIMARY KEY,
        v TEXT
    )""")
    conn.commit()

def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_meta(conn: sqlite3.Connection, key: str, val: str):
    cur = conn.cursor()
    cur.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key,val))
    conn.commit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True, help="events jsonl from parse_schedule.py")
    ap.add_argument("--store-dir", required=True, help="directory for FAISS/SQLite")
    ap.add_argument("--local-emb", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--append", action="store_true", help="append into existing FAISS/SQLite instead of rebuilding")
    ap.add_argument("--no-dedupe", action="store_true", help="disable duplicate checking by hash")
    args = ap.parse_args()

    os.makedirs(args.store_dir, exist_ok=True)
    sqlite_path = os.path.join(args.store_dir, "chunks.sqlite")
    faiss_path  = os.path.join(args.store_dir, "index.faiss")

    # ----- Load events -----
    events = []
    with open(args.jsonl, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))
    if not events:
        raise SystemExit("No events found in JSONL. Check parse step.")

    # ----- Open SQLite & ensure schema -----
    conn = sqlite3.connect(sqlite_path)
    ensure_schema(conn)
    cur = conn.cursor()

    # ----- Prepare FAISS index -----
    model = SentenceTransformer(args.local_emb)
    dim = getattr(model, "get_sentence_embedding_dimension", lambda: None)() or model.encode(["x"]).shape[1]

    if args.append and os.path.exists(faiss_path):
        index = faiss.read_index(faiss_path)
        n_old = index.ntotal

        # kiểm tra tương thích model + dim
        prev_model = get_meta(conn, "emb_model")
        prev_dim   = get_meta(conn, "emb_dim")
        if prev_model and prev_model != args.local_emb:
            raise SystemExit(f"[ERR] Embedding model mismatch: store='{prev_model}', given='{args.local_emb}'. Run without --append to rebuild.")
        if prev_dim and int(prev_dim) != dim:
            raise SystemExit(f"[ERR] Embedding dim mismatch: store={prev_dim}, model={dim}. Run without --append to rebuild.")

        # sanity check số bản ghi trong SQLite khớp ntotal
        cur.execute("SELECT COUNT(*) FROM chunks")
        rows_cnt = cur.fetchone()[0]
        if rows_cnt != n_old:
            raise SystemExit(f"[ERR] Inconsistent state: FAISS ntotal={n_old} but SQLite rows={rows_cnt}. "
                             f"Please rebuild (run without --append) to resync.")
    else:
        # build mới
        index = faiss.IndexFlatIP(dim)
        n_old = 0
        # clear SQLite nếu không append
        cur.execute("DELETE FROM chunks")
        conn.commit()

    # lưu meta (sau khi đã pass các checks)
    set_meta(conn, "emb_model", args.local_emb)
    set_meta(conn, "emb_dim", str(dim))

    # ----- Dedupe -----
    # 1) Dedupe so với DB (trừ khi --no-dedupe)
    existing_hashes = set()
    if not args.no_dedupe:
        for (h,) in cur.execute("SELECT hash FROM chunks"):
            existing_hashes.add(h)

    # 2) Dedupe trong batch (tránh lặp trong file JSONL)
    batch_seen = set()

    new_records = []  # (hash, text, ev)
    for ev in events:
        txt = chunk_text_fields(ev)
        h = sha1(txt)
        if args.no_dedupe:
            if h in batch_seen:
                continue
            batch_seen.add(h)
            new_records.append((h, txt, ev))
        else:
            if h in existing_hashes or h in batch_seen:
                continue
            batch_seen.add(h)
            new_records.append((h, txt, ev))

    if not new_records:
        print("[OK] Nothing new to ingest (all duplicates).")
        print("[OK] FAISS:", faiss_path)
        print("[OK] SQLite:", sqlite_path)
        conn.close()
        raise SystemExit(0)

    texts = [r[1] for r in new_records]
    embs = model.encode(texts, normalize_embeddings=True)
    embs = np.asarray(embs, dtype="float32")

    # ----- Append vectors -----
    before = index.ntotal
    index.add(embs)
    after = index.ntotal
    if after - before != len(new_records):
        raise SystemExit(f"[ERR] FAISS add mismatch: expected +{len(new_records)} but got +{after-before}.")

    faiss.write_index(index, faiss_path)

    # ----- Insert metadata rows with stable IDs (offset by n_old) -----
    # id phải chạy liên tục từ 0..index.ntotal-1
    start_id = n_old
    rows = []
    for i, (h, txt, ev) in enumerate(new_records):
        rid = start_id + i
        rows.append((
            rid, h, txt,
            ev.get("date"), ev.get("dow"), ev.get("start"), ev.get("end"),
            ev.get("location"), ev.get("participants"), ev.get("title"), ev.get("raw")
        ))

    cur.executemany("""INSERT INTO chunks(
        id, hash, text, date, dow, start, end, location, participants, title, raw
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()

    # verify: số hàng trong SQLite phải == index.ntotal
    cur.execute("SELECT COUNT(*) FROM chunks")
    rows_cnt_after = cur.fetchone()[0]
    if rows_cnt_after != index.ntotal:
        raise SystemExit(f"[ERR] Post-insert mismatch: SQLite rows={rows_cnt_after} vs FAISS ntotal={index.ntotal}. "
                         f"Stop to avoid corrupted mapping.")

    conn.close()

    print(f"[OK] Stored {len(new_records)} new chunks (total was {n_old}, now {n_old + len(new_records)})")
    print("[OK] FAISS:", faiss_path)
    print("[OK] SQLite:", sqlite_path)