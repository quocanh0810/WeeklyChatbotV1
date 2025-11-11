# rag/io_store.py
from __future__ import annotations
import sqlite3
from typing import Dict, List, Optional, Tuple
import faiss, numpy as np
from functools import lru_cache

from .settings import SQLITE_PATH, FAISS_PATH, LOCAL_EMB_MODEL

# SQLite
def get_events_by_date(date_str: str) -> List[Dict]:
    conn = sqlite3.connect(SQLITE_PATH); cur = conn.cursor()
    cur.execute(
        """
        SELECT id, text, date, dow, start, end, location, participants, title, raw
        FROM chunks
        WHERE date = ?
        ORDER BY 
            CASE WHEN start IS NULL OR TRIM(start)='' THEN 1 ELSE 0 END,
            start,
            id
        """,
        (date_str,),
    )
    rows = cur.fetchall(); conn.close()
    return [
        {"id": r[0], "text": r[1], "date": r[2], "dow": r[3], "start": r[4],
         "end": r[5], "location": r[6], "participants": r[7], "title": r[8], "raw": r[9]}
        for r in rows
    ]

def list_all_dates() -> List[str]:
    conn = sqlite3.connect(SQLITE_PATH); cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM chunks"); dates = [r[0] for r in cur.fetchall() if r[0]]
    conn.close()
    return dates

def _fetch_all_date_dow_pairs() -> List[Tuple[str, str]]:
    conn = sqlite3.connect(SQLITE_PATH); cur = conn.cursor()
    cur.execute("SELECT DISTINCT date, dow FROM chunks"); pairs = cur.fetchall()
    conn.close()
    return [(d, dw) for (d, dw) in pairs if d and dw]

# ---------- FAISS ----------
_index = faiss.read_index(FAISS_PATH)

@lru_cache(maxsize=1)
def _st_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(LOCAL_EMB_MODEL)

def vector_search(q: str, k: int = 10) -> List[Dict]:
    v = _st_model().encode([q], normalize_embeddings=True)
    D, I = _index.search(np.asarray(v, dtype="float32"), k)
    rows = []
    conn = sqlite3.connect(SQLITE_PATH); cur = conn.cursor()
    for idx, score in zip(I[0].tolist(), D[0].tolist()):
        cur.execute("""SELECT id,text,date,dow,start,end,location,participants,title,raw 
                       FROM chunks WHERE id=?""", (int(idx),))
        r = cur.fetchone()
        if r:
            rows.append({"id": r[0], "text": r[1], "date": r[2], "dow": r[3], "start": r[4],
                         "end": r[5], "location": r[6], "participants": r[7], "title": r[8],
                         "raw": r[9], "score": float(score)})
    conn.close()
    return rows