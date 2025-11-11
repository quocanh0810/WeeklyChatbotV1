# rag/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip()

STORE_DIR   = os.getenv("STORE_DIR", "rag_store")
SQLITE_PATH = os.path.join(STORE_DIR, "chunks.sqlite")
FAISS_PATH  = os.path.join(STORE_DIR, "index.faiss")

LOCAL_EMB_MODEL = os.getenv("LOCAL_EMB_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

if not GEMINI_API_KEY:
    raise RuntimeError("Missing GEMINI_API_KEY in .env")
if not os.path.exists(SQLITE_PATH):
    raise RuntimeError(f"SQLite DB not found: {SQLITE_PATH}")
if not os.path.exists(FAISS_PATH):
    raise RuntimeError(f"FAISS index not found: {FAISS_PATH}")