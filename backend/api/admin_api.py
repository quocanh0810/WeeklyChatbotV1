# backend/api/admin_api.py
from __future__ import annotations
import os, json, datetime as dt, sqlite3
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, BackgroundTasks

# Imports theo gói backend
from backend.api.admin_auth import require_admin, make_token, ADMIN_USER, ADMIN_PASS
from backend.rag.parser import parse_docx_as_table, infer_year_from_doc
from backend.ingest.ingest_lib import append_events, rebuild_events

from fastapi import Query

# Đường dẫn theo project_root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
UPLOAD_DIR   = PROJECT_ROOT / "data" / "uploads"
STORE_DIR    = os.getenv("STORE_DIR", str(PROJECT_ROOT / "rag_store"))
DB_PATH      = str(Path(STORE_DIR) / "chunks.sqlite")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

router = APIRouter(prefix="/api/admin", tags=["admin"])

@router.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    if username != ADMIN_USER or password != ADMIN_PASS:
        raise HTTPException(401, "Bad credentials")
    return {"token": make_token(username)}

@router.post("/upload/preview")
def upload_preview(
    file: UploadFile = File(...),
    year: int | None = Form(None),
    admin: str = Depends(require_admin),
):
    safe_name = Path(file.filename).name
    if not safe_name.lower().endswith(".docx"):
        raise HTTPException(400, "Only .docx is supported")

    tmp_name = f"upload_{int(dt.datetime.now().timestamp())}_{safe_name}"
    tmp_path = UPLOAD_DIR / tmp_name
    tmp_path.write_bytes(file.file.read())

    try:
        from docx import Document
        doc = Document(tmp_path.as_posix())
        default_year = year or infer_year_from_doc(doc) or dt.date.today().year
        events = parse_docx_as_table(tmp_path.as_posix(), default_year)
    except Exception as e:
        raise HTTPException(400, f"parse_error: {e}")

    return {
        "file": safe_name,
        "temp_path": tmp_path.as_posix(),
        "count": len(events),
        "events": events[:300],
    }

@router.post("/ingest")
def do_ingest(
    bg: BackgroundTasks,
    temp_path: str = Form(...),
    mode: str = Form("append"),           # append | rebuild
    tag: str | None = Form(None),
    dedupe: bool = True,
    admin: str = Depends(require_admin),
):
    p = Path(temp_path)
    if not p.is_absolute():
        p = (UPLOAD_DIR / p.name).resolve()
    elif not str(p).startswith(str(UPLOAD_DIR.resolve())):
        raise HTTPException(400, detail="temp_path must be inside uploads directory")

    if not p.exists():
        raise HTTPException(400, detail=f"temp_path invalid or not found: {p.as_posix()}")

    mode = (mode or "append").lower()
    if mode not in ("append", "rebuild"):
        raise HTTPException(400, detail="mode must be 'append' or 'rebuild'")

    task_id = int(dt.datetime.now().timestamp())
    # log trạng thái queued để UI thấy ngay
    _log_upload(task_id, filename=p.name, tag=tag, mode=mode, status="queued")
    bg.add_task(_ingest_task, p.as_posix(), mode, tag, dedupe, task_id)
    return {"task_id": task_id, "status": "queued"}

def _ingest_task(temp_path: str, mode: str, tag: str | None, dedupe: bool, task_id: int):
    try:
        p = Path(temp_path)
        if not p.exists():
            raise FileNotFoundError(f"temp_path not found: {temp_path!r}")

        from docx import Document
        import datetime as _dt
        from backend.rag.parser import parse_docx_as_table, infer_year_from_doc

        doc = Document(p.as_posix())
        default_year = infer_year_from_doc(doc) or _dt.date.today().year
        events = parse_docx_as_table(p.as_posix(), default_year)

        if mode == "rebuild":
            res = rebuild_events(events, STORE_DIR)
        else:
            res = append_events(events, STORE_DIR, dedupe=dedupe)

        _log_upload(
            task_id,
            filename=p.name,
            tag=tag,
            mode=mode,
            added=res.get("added", 0),
            total=res.get("total_after", 0),
            status="done",
            log=json.dumps(res, ensure_ascii=False),
        )
    except Exception:
        import traceback
        err = traceback.format_exc()
        _log_upload(task_id, status="failed", log=err)
        print("[INGEST_TASK][FAILED]\n", err)

def _log_upload(task_id: int, filename: str | None=None, tag: str | None=None, mode: str | None=None,
                status: str="queued", added: int | None=None, total: int | None=None, log: str | None=None):
    Path(STORE_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS uploads(
      id INTEGER PRIMARY KEY,
      filename TEXT, tag TEXT, mode TEXT, total_events INTEGER, added_events INTEGER,
      status TEXT, log TEXT, created_at TEXT, updated_at TEXT)""")
    now = dt.datetime.now().isoformat(timespec="seconds")
    cur = conn.cursor()
    cur.execute("SELECT id FROM uploads WHERE id=?", (task_id,))
    if cur.fetchone():
        cur.execute("""UPDATE uploads SET status=?, added_events=COALESCE(?,added_events),
                       total_events=COALESCE(?,total_events), log=COALESCE(?,log), updated_at=? WHERE id=?""",
                    (status, added, total, log, now, task_id))
    else:
        cur.execute("""INSERT INTO uploads(id,filename,tag,mode,total_events,added_events,status,log,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (task_id, filename, tag, mode, total, added, status, log, now, now))
    conn.commit(); conn.close()

@router.get("/uploads")
def list_uploads(admin: str = Depends(require_admin)):
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM uploads ORDER BY id DESC LIMIT 50")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"items": rows}

# Phân trang

from fastapi import Query

@router.get("/uploads")
def list_uploads(
    admin: str = Depends(require_admin),
    page: int = Query(1, ge=1),
    page_size: int = Query(8, ge=1, le=200),         # mặc định 8
    tag: str | None = Query(None),
):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # total
    if tag:
        cur.execute("SELECT COUNT(*) FROM uploads WHERE COALESCE(tag,'') = ?", (tag,))
    else:
        cur.execute("SELECT COUNT(*) FROM uploads")
    total = cur.fetchone()[0]

    # normalize
    total_pages = max((total + page_size - 1) // page_size, 1)
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * page_size

    # fetch page
    if tag:
        cur.execute(
            """SELECT * FROM uploads
               WHERE COALESCE(tag,'') = ?
               ORDER BY id DESC
               LIMIT ? OFFSET ?""",
            (tag, page_size, offset),
        )
    else:
        cur.execute(
            """SELECT * FROM uploads
               ORDER BY id DESC
               LIMIT ? OFFSET ?""",
            (page_size, offset),
        )
    items = [dict(r) for r in cur.fetchall()]
    conn.close()

    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }