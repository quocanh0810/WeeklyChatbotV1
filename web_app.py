from dotenv import load_dotenv
load_dotenv() 

# web_app.py
import os, traceback
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

# ========= Lazy import RAG =========
RAGAsk = None
rag_ask = None
rag_import_error = None

def _lazy_import_rag():
    """Import trễ rag.service để tránh crash khi kho/index chưa sẵn."""
    global RAGAsk, rag_ask, rag_import_error
    if RAGAsk and rag_ask:
        return
    try:
        from rag.service import Ask as _Ask, ask as _ask  # noqa
        RAGAsk = _Ask
        rag_ask = _ask
        rag_import_error = None
    except Exception as e:
        rag_import_error = f"{e}\n{traceback.format_exc()}"

# ========= App =========
app = FastAPI(title="TMU Weekly Bot", version="1.0.0")

# CORS (có thể cấu hình qua ENV)
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOW_ORIGINS if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========= Static: web (frontend) =========
WEB_DIR = "web"
os.makedirs(WEB_DIR, exist_ok=True)
app.mount("/assets", StaticFiles(directory=WEB_DIR, html=False), name="assets")

@app.get("/")
def serve_index():
    index_path = os.path.join(WEB_DIR, "index.html")
    if not os.path.exists(index_path):
        return {"message": "Put your frontend in ./web (index.html, main.js, style.css)."}
    return FileResponse(index_path)

# Favicon để khỏi 404 spam log
@app.get("/favicon.ico")
def favicon():
    fav_path = os.path.join(WEB_DIR, "favicon.ico")
    if os.path.exists(fav_path):
        return FileResponse(fav_path)
    # Không có favicon → trả rỗng
    return Response(status_code=204)

# ========= (tuỳ chọn) Dashboard tĩnh ở /admin =========
WEB_ADMIN_DIR = "web_admin"
if os.path.isdir(WEB_ADMIN_DIR):
    app.mount("/admin", StaticFiles(directory=WEB_ADMIN_DIR, html=True), name="admin")

    @app.get("/admin/")
    def admin_index():
        idx = os.path.join(WEB_ADMIN_DIR, "index.html")
        if os.path.exists(idx):
            return FileResponse(idx)
        return {"message": "Put your admin dashboard in ./web_admin/index.html"}

# ========= (tuỳ chọn) API quản trị =========
try:
    from admin_api import router as admin_router  # absolute import để chạy uvicorn web_app:app ở root
    app.include_router(admin_router)
except Exception:
    # Không chặn app nếu phần admin chưa sẵn
    pass

# ========= Health & Version =========
@app.get("/version")
def version():
    return {"name": "TMU Weekly Bot", "version": app.version}

@app.get("/health")
def health():
    _lazy_import_rag()
    if rag_import_error:
        return {"status": "degraded", "detail": "rag not ready", "error": rag_import_error}
    return {"status": "ok"}

# ========= Chat API =========
class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    answer: str

@app.post("/api/chat", response_model=ChatResponse)
def api_chat(req: ChatRequest):
    _lazy_import_rag()
    if rag_import_error:
        raise HTTPException(status_code=500, detail=f"RAG init failed: {rag_import_error}")

    msg = (req.message or "").strip()
    if not msg:
        raise HTTPException(status_code=400, detail="message is empty")

    try:
        res = rag_ask(RAGAsk(question=msg))  # dict {"answer": ..., "hits": ...}
        answer = (res.get("answer") or "").strip()
        if not answer:
            answer = "Mình không tìm thấy thông tin trong lịch tuần này."
        return ChatResponse(answer=answer)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"internal_error: {e}")

# ========= Tương thích client_cli.py cũ =========
class AskIn(BaseModel):
    question: str

@app.post("/ask")
def api_ask_compat(req: AskIn):
    _lazy_import_rag()
    if rag_import_error:
        raise HTTPException(status_code=500, detail=f"RAG init failed: {rag_import_error}")
    try:
        return rag_ask(RAGAsk(question=req.question))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"internal_error: {e}")