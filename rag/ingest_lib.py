# rag/ingest_lib.py
from __future__ import annotations

import os
import hashlib
import sqlite3
from typing import List, Dict, Tuple

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


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


# ====== UTILITIES =============================================================

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


# ====== CORE: APPEND / REBUILD ===============================================

def append_events(
    events: List[Dict],
    store_dir: str,
    local_emb: str = "sentence-transformers/all-MiniLM-L6-v2",
    dedupe: bool = True,
) -> Dict:
    """
    Append-safe ingestion:
      - Duy trì ánh xạ: FAISS vector index i  <->  SQLite.chunks.id = i
      - Kiểm tra model/embedding dim nhất quán qua bảng meta
      - Dedupe theo hash nội dung (UNIQUE)
      - Đồng bộ: số row SQLite phải == index.ntotal
    Trả về summary:
      {added, total_before, total_after, sqlite_path, faiss_path}
    """
    sqlite_path, faiss_path = _paths(store_dir)

    # 1) SQLite & schema
    conn = sqlite3.connect(sqlite_path)
    _ensure_schema(conn)
    cur = conn.cursor()

    # 2) Chuẩn bị FAISS index & check meta
    model = SentenceTransformer(local_emb)
    try:
        dim = model.get_sentence_embedding_dimension()
    except Exception:
        # fallback (hiếm): encode 1 câu để suy ra dim
        dim = int(model.encode(["a"]).shape[1])

    if os.path.exists(faiss_path):
        index = faiss.read_index(faiss_path)
        n_old = index.ntotal
        # kiểm tra meta
        prev_model = _get_meta(conn, "emb_model")
        prev_dim   = _get_meta(conn, "emb_dim")
        if prev_model and prev_model != local_emb:
            conn.close()
            raise SystemExit(
                f"[ERR] Embedding model mismatch: store='{prev_model}' vs now='{local_emb}'"
            )
        if prev_dim and int(prev_dim) != dim:
            conn.close()
            raise SystemExit(
                f"[ERR] Embedding dim mismatch: store={prev_dim} vs now={dim}"
            )
    else:
        index = faiss.IndexFlatIP(dim)
        n_old = 0

    # 3) Đồng bộ an toàn trước khi append
    cur.execute("SELECT COUNT(*) FROM chunks")
    rows_cnt_before = cur.fetchone()[0]
    if rows_cnt_before != n_old:
        conn.close()
        raise SystemExit(
            f"[ERR] Pre-append mismatch: SQLite rows={rows_cnt_before} vs FAISS ntotal={n_old}. "
            "The mapping id<->vector is corrupted."
        )

    # 4) Chuẩn bị dữ liệu mới (hash, text)
    pending = _load_events_texts(events)

    # 5) Dedupe theo hash (chỉ những hash chưa có)
    if dedupe:
        existing = set()
        for (h,) in cur.execute("SELECT hash FROM chunks"):
            if h:
                existing.add(h)
        new_records = [(h, txt, ev) for (h, txt, ev) in pending if h not in existing]
    else:
        new_records = pending

    if not new_records:
        # không có gì mới
        _set_meta(conn, "emb_model", local_emb)
        _set_meta(conn, "emb_dim", str(dim))
        conn.commit()
        conn.close()
        return {
            "added": 0,
            "total_before": n_old,
            "total_after": n_old,
            "sqlite_path": sqlite_path,
            "faiss_path": faiss_path,
        }

    # 6) Tính embedding & append vào FAISS
    texts = [r[1] for r in new_records]
    embs = model.encode(texts, normalize_embeddings=True)
    embs = np.asarray(embs, dtype="float32")
    if embs.shape[1] != dim:
        conn.close()
        raise SystemExit(f"[ERR] Embedding dim {embs.shape[1]} != expected {dim}")

    index.add(embs)
    faiss.write_index(index, faiss_path)

    # 7) Ghi metadata vào SQLite với id = offset + i
    rows = []
    for i, (h, txt, ev) in enumerate(new_records):
        rid = n_old + i
        rows.append((
            rid, txt,
            ev.get("date"), ev.get("dow"), ev.get("start"), ev.get("end"),
            ev.get("location"), ev.get("participants"), ev.get("title"), ev.get("raw"),
            h
        ))

    cur.executemany("""
        INSERT OR IGNORE INTO chunks(
            id, text, date, dow, start, end, location, participants, title, raw, hash
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    # 8) Lưu meta & sanity check sau khi chèn
    _set_meta(conn, "emb_model", local_emb)
    _set_meta(conn, "emb_dim", str(dim))

    cur.execute("SELECT COUNT(*) FROM chunks")
    rows_cnt_after = cur.fetchone()[0]
    if rows_cnt_after != index.ntotal:
        conn.close()
        raise SystemExit(
            f"[ERR] Post-append mismatch: SQLite rows={rows_cnt_after} vs FAISS ntotal={index.ntotal}. "
            "Stop to avoid corrupted mapping."
        )

    conn.close()
    return {
        "added": len(new_records),
        "total_before": n_old,
        "total_after": rows_cnt_after,
        "sqlite_path": sqlite_path,
        "faiss_path": faiss_path,
    }


def rebuild_events(
    events: List[Dict],
    store_dir: str,
    local_emb: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> Dict:
    """
    Xoá & nạp lại toàn bộ chỉ từ 'events' (dùng khi muốn làm sạch store).
    CẢNH BÁO: sẽ thay thế index.faiss và bảng chunks.
    """
    sqlite_path, faiss_path = _paths(store_dir)

    # 1) Chuẩn bị DB sạch
    conn = sqlite3.connect(sqlite_path)
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("DELETE FROM chunks")
    conn.commit()

    # 2) Chuẩn bị model/index mới
    model = SentenceTransformer(local_emb)
    try:
        dim = model.get_sentence_embedding_dimension()
    except Exception:
        dim = int(model.encode(["a"]).shape[1])
    index = faiss.IndexFlatIP(dim)

    # 3) Tính embedding đầy đủ
    pending = _load_events_texts(events)
    texts = [r[1] for r in pending]
    embs = model.encode(texts, normalize_embeddings=True)
    embs = np.asarray(embs, dtype="float32")

    # 4) Ghi vào FAISS
    index.add(embs)
    faiss.write_index(index, faiss_path)

    # 5) Ghi vào SQLite (id = vị trí vector)
    rows = []
    for i, (h, txt, ev) in enumerate(pending):
        rows.append((
            i, txt,
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

    # 6) Lưu meta & sanity check
    _set_meta(conn, "emb_model", local_emb)
    _set_meta(conn, "emb_dim", str(dim))

    cur.execute("SELECT COUNT(*) FROM chunks")
    rows_cnt = cur.fetchone()[0]
    if rows_cnt != index.ntotal:
        conn.close()
        raise SystemExit(
            f"[ERR] Post-rebuild mismatch: SQLite rows={rows_cnt} vs FAISS ntotal={index.ntotal}."
        )

    conn.close()
    return {
        "added": len(pending),
        "total_before": 0,
        "total_after": rows_cnt,
        "sqlite_path": sqlite_path,
        "faiss_path": faiss_path,
    }