# rag/ingest_lib.py
from __future__ import annotations

import os
import hashlib
import sqlite3
from typing import List, Dict, Tuple

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# thêm ở đầu file (tiện ích nhỏ)
def _backfill_hashes(conn: sqlite3.Connection):
    """Điền hash cho các dòng cũ chưa có hash để dedupe chuẩn."""
    cur = conn.cursor()
    cur.execute("SELECT id, text FROM chunks WHERE hash IS NULL OR TRIM(hash)=''")
    rows = cur.fetchall()
    if not rows:
        return 0
    for rid, txt in rows:
        h = _sha1(txt or "")
        cur.execute("UPDATE chunks SET hash=? WHERE id=?", (h, rid))
    conn.commit()
    return len(rows)

def _rebuild_faiss_from_sqlite(conn: sqlite3.Connection, faiss_path: str,
                               model: SentenceTransformer) -> int:
    """Khi lệch rows vs ntotal, build lại FAISS theo SQLite để đồng bộ."""
    cur = conn.cursor()
    cur.execute("SELECT id, text FROM chunks ORDER BY id ASC")
    rows = cur.fetchall()
    if not rows:
        index = faiss.IndexFlatIP(model.get_sentence_embedding_dimension())
        faiss.write_index(index, faiss_path)
        return 0
    ids, texts = zip(*rows)
    # đảm bảo id = 0..n-1 liên tục; nếu không, reindex
    need_reindex = any(i != idx for idx, i in enumerate(ids))
    if need_reindex:
        # tạo bảng tạm và ghi lại id liên tục
        cur.execute("BEGIN")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chunks_new(
              id INTEGER PRIMARY KEY,
              text TEXT,
              date TEXT, dow TEXT, start TEXT, end TEXT,
              location TEXT, participants TEXT, title TEXT, raw TEXT,
              hash TEXT
            )
        """)
        cur.execute("INSERT INTO chunks_new(id,text,date,dow,start,end,location,participants,title,raw,hash) "
                    "SELECT ROW_NUMBER() OVER (ORDER BY id)-1, text,date,dow,start,end,location,participants,title,raw,hash FROM chunks")
        # SQLite không có WINDOW ROW_NUMBER() cũ; fallback thủ công:
        cur.execute("DELETE FROM chunks_new")  # xoá nếu dòng trên không chạy trên SQLite cũ
        cur.execute("SELECT text,date,dow,start,end,location,participants,title,raw,hash FROM chunks ORDER BY id")
        data = cur.fetchall()
        cur.execute("DELETE FROM chunks")
        for i, row in enumerate(data):
            cur.execute("""INSERT OR REPLACE INTO chunks(
                id,text,date,dow,start,end,location,participants,title,raw,hash
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (i,)+row)
        conn.commit()
        # load lại texts sau reindex
        cur.execute("SELECT id, text FROM chunks ORDER BY id ASC")
        rows = cur.fetchall()
        ids, texts = zip(*rows)

    embs = model.encode(list(texts), normalize_embeddings=True)
    embs = np.asarray(embs, dtype="float32")
    index = faiss.IndexFlatIP(embs.shape[1])
    index.add(embs)
    faiss.write_index(index, faiss_path)
    return index.ntotal

# ====== ĐƯỜNG DẪN / SCHEMA ====================================================

def _paths(store_dir: str) -> Tuple[str, str]:
    os.makedirs(store_dir, exist_ok=True)
    sqlite_path = os.path.join(store_dir, "chunks.sqlite")
    faiss_path  = os.path.join(store_dir, "index.faiss")
    return sqlite_path, faiss_path


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Tạo bảng nếu thiếu và thêm cột hash nếu DB cũ vẫn chưa có (migrate nhẹ)."""
    cur = conn.cursor()
    # bảng metadata khoá/giá trị
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta(
      k TEXT PRIMARY KEY,
      v TEXT
    );
    """)

    # bảng dữ liệu chính
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chunks(
      id INTEGER PRIMARY KEY,
      text TEXT,
      date TEXT, dow TEXT, start TEXT, end TEXT,
      location TEXT, participants TEXT, title TEXT, raw TEXT
    );
    """)

    # thêm cột hash (nếu thiếu)
    cur.execute("PRAGMA table_info(chunks)")
    cols = {r[1] for r in cur.fetchall()}
    if "hash" not in cols:
        cur.execute("ALTER TABLE chunks ADD COLUMN hash TEXT")
        # có thể điền dần hash về sau; UNIQUE sẽ tạo trên cột hash để dedupe nhanh
    # đảm bảo chỉ mục unique cho hash (nếu chưa có)
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_chunks_hash_unique
      ON chunks(hash);
    """)

    conn.commit()


def _get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def _set_meta(conn: sqlite3.Connection, key: str, val: str) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO meta(k, v) VALUES(?, ?)
        ON CONFLICT(k) DO UPDATE SET v=excluded.v
    """, (key, val))
    conn.commit()


# UTILITIES 

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _chunk_text_fields(ev: Dict) -> str:
    """Ghép các trường có giá trị thành 1 đoạn văn để embedding."""
    fields = []
    for k in ("date", "dow", "start", "end", "location", "participants", "title"):
        v = ev.get(k)
        if v:
            fields.append(f"{k}: {v}")
    if ev.get("raw"):
        fields.append(f"raw: {ev['raw']}")
    return "\n".join(fields)


def _load_events_texts(events: List[Dict]) -> List[Tuple[str, str, Dict]]:
    """Trả về list (hash, text, ev)."""
    out = []
    for ev in events:
        txt = _chunk_text_fields(ev)
        h = _sha1(txt)
        out.append((h, txt, ev))
    return out


# CORE: APPEND / REBUILD

def append_events(
    events: List[Dict],
    store_dir: str,
    local_emb: str = "sentence-transformers/all-MiniLM-L6-v2",
    dedupe: bool = True,
) -> Dict:
    sqlite_path, faiss_path = _paths(store_dir)

    conn = sqlite3.connect(sqlite_path)
    _ensure_schema(conn)
    cur = conn.cursor()

    # backfill hash cho DB cũ (giúp dedupe hoạt động chuẩn)
    _backfill_hashes(conn)

    model = SentenceTransformer(local_emb)
    try:
        dim = model.get_sentence_embedding_dimension()
    except Exception:
        dim = int(model.encode(["a"]).shape[1])

    if os.path.exists(faiss_path):
        index = faiss.read_index(faiss_path)
        n_old = index.ntotal
    else:
        index = faiss.IndexFlatIP(dim)
        n_old = 0

    # meta nhất quán
    prev_model = _get_meta(conn, "emb_model")
    prev_dim   = _get_meta(conn, "emb_dim")
    if prev_model and prev_model != local_emb:
        # tự rebuild FAISS theo SQLite cho an toàn
        n_old = _rebuild_faiss_from_sqlite(conn, faiss_path, model)
        index = faiss.read_index(faiss_path)
    if prev_dim and prev_dim != str(dim):
        n_old = _rebuild_faiss_from_sqlite(conn, faiss_path, model)
        index = faiss.read_index(faiss_path)

    # sanity: đồng bộ rows vs ntotal (tự-heal nếu lệch)
    cur.execute("SELECT COUNT(*) FROM chunks")
    rows_cnt_before = cur.fetchone()[0]
    if rows_cnt_before != n_old:
        n_old = _rebuild_faiss_from_sqlite(conn, faiss_path, model)
        index = faiss.read_index(faiss_path)
        cur.execute("SELECT COUNT(*) FROM chunks")
        rows_cnt_before = cur.fetchone()[0]

    # materialize records
    pending = _load_events_texts(events)

    if dedupe:
        existing = set(h for (h,) in cur.execute("SELECT hash FROM chunks") if h)
        new_records = [(h, txt, ev) for (h, txt, ev) in pending if h not in existing]
    else:
        new_records = pending

    if not new_records:
        _set_meta(conn, "emb_model", local_emb)
        _set_meta(conn, "emb_dim", str(dim))
        conn.commit(); conn.close()
        return {
            "added": 0,
            "total_before": rows_cnt_before,
            "total_after": rows_cnt_before,
            "sqlite_path": sqlite_path,
            "faiss_path": faiss_path,
        }

    # encode + add
    texts = [r[1] for r in new_records]
    embs = model.encode(texts, normalize_embeddings=True)
    embs = np.asarray(embs, dtype="float32")
    if embs.shape[1] != dim:
        # rebuild rồi thử lại 1 lần
        _rebuild_faiss_from_sqlite(conn, faiss_path, model)
        index = faiss.read_index(faiss_path)

    index.add(embs)
    faiss.write_index(index, faiss_path)

    rows = []
    for i, (h, txt, ev) in enumerate(new_records):
        rid = rows_cnt_before + i
        rows.append((
            rid, txt,
            ev.get("date"), ev.get("dow"), ev.get("start"), ev.get("end"),
            ev.get("location"), ev.get("participants"), ev.get("title"), ev.get("raw"),
            h
        ))
    cur.executemany("""
        INSERT OR REPLACE INTO chunks(
            id, text, date, dow, start, end, location, participants, title, raw, hash
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    _set_meta(conn, "emb_model", local_emb)
    _set_meta(conn, "emb_dim", str(dim))

    cur.execute("SELECT COUNT(*) FROM chunks")
    rows_cnt_after = cur.fetchone()[0]
    # soft check: không “die”, chỉ cảnh báo nếu lệch
    warn = None
    if rows_cnt_after != index.ntotal:
        warn = f"warning: sqlite_rows={rows_cnt_after} vs faiss_ntotal={index.ntotal}"

    conn.close()
    return {
        "added": len(new_records),
        "total_before": rows_cnt_before,
        "total_after": rows_cnt_after,
        "sqlite_path": sqlite_path,
        "faiss_path": faiss_path,
        "warning": warn
    }

# rag/ingest_lib.py (chỉ phần rebuild_events)

def rebuild_events(events: list[dict], store_dir: str,
                   local_emb: str = "sentence-transformers/all-MiniLM-L6-v2",
                   dedupe: bool = True) -> dict:
    import os, sqlite3, hashlib, numpy as np, faiss
    from sentence_transformers import SentenceTransformer

    def sha1(s: str) -> str:
        import hashlib
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    sqlite_path = os.path.join(store_dir, "chunks.sqlite")
    faiss_path  = os.path.join(store_dir, "index.faiss")
    os.makedirs(store_dir, exist_ok=True)

    # mở SQLite
    conn = sqlite3.connect(sqlite_path)
    cur  = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chunks(
      id INTEGER PRIMARY KEY,
      hash TEXT UNIQUE,
      text TEXT,
      date TEXT, dow TEXT, start TEXT, end TEXT,
      location TEXT, participants TEXT, title TEXT, raw TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)""")
    conn.commit()

    # clear dữ liệu cũ
    cur.execute("DELETE FROM chunks")
    conn.commit()

    # chuẩn bị texts + dedupe theo hash
    def materialize_text(ev):
        parts=[]
        for k in ("date","dow","start","end","location","participants","title"):
            v=ev.get(k)
            if v: parts.append(f"{k}: {v}")
        if ev.get("raw"): parts.append(f"raw: {ev['raw']}")
        return "\n".join(parts)

    records=[]
    if dedupe:
        seen=set()
        for ev in events:
            txt = materialize_text(ev)
            h = sha1(txt)
            if h in seen: 
                continue
            seen.add(h)
            records.append((h, txt, ev))
    else:
        for ev in events:
            txt = materialize_text(ev)
            h   = sha1(txt)
            records.append((h, txt, ev))

    # tạo FAISS mới
    model = SentenceTransformer(local_emb)
    dim   = model.get_sentence_embedding_dimension()
    index = faiss.IndexFlatIP(dim)

    # encode + add
    texts = [r[1] for r in records]
    if texts:
        embs  = model.encode(texts, normalize_embeddings=True)
        embs  = np.asarray(embs, dtype="float32")
        index.add(embs)
    faiss.write_index(index, faiss_path)

    # insert rows với id khớp thứ tự index
    for i, (h, txt, ev) in enumerate(records):
        cur.execute("""INSERT OR REPLACE INTO chunks(
            id, hash, text, date, dow, start, end, location, participants, title, raw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (i, h, txt, ev.get("date"), ev.get("dow"), ev.get("start"), ev.get("end"),
         ev.get("location"), ev.get("participants"), ev.get("title"), ev.get("raw")))
    conn.commit()

    # lưu meta
    cur.execute("INSERT INTO meta(k,v) VALUES('emb_model',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (local_emb,))
    cur.execute("INSERT INTO meta(k,v) VALUES('emb_dim',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (str(dim),))
    conn.commit()

    # kiểm tra “mềm” và trả summary
    rows_cnt = cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    ntotal   = index.ntotal
    conn.close()

    ok = (rows_cnt == ntotal)
    warn = None if ok else f"warning: sqlite_rows={rows_cnt} vs faiss_ntotal={ntotal}"

    return {
        "mode": "rebuild",
        "added": len(records),
        "total_before": 0,
        "total_after": rows_cnt,
        "sqlite_path": sqlite_path,
        "faiss_path": faiss_path,
        "ok": ok,
        "warning": warn
    }