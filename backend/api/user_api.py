# backend/api/user_api.py
from __future__ import annotations
import traceback
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import os

# Lazy Import RAG
RAGAsk = None
rag_ask = None
rag_import_error = None

def _lazy_import_rag():
    """Import trễ để tránh crash khi FAISS/chunks chưa build."""
    global RAGAsk, rag_ask, rag_import_error
    if RAGAsk and rag_ask:
        return
    try:
        from backend.rag.service import Ask as _Ask, ask as _ask
        RAGAsk = _Ask
        rag_ask = _ask
        rag_import_error = None
    except Exception as e:
        rag_import_error = f"{e}\n{traceback.format_exc()}"

# ========== Router ==========
router = APIRouter(prefix="/api", tags=["chat"])

# Request/Response Model
class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    answer: str

@router.post("/chat", response_model=ChatResponse)
def api_chat(req: ChatRequest):
    _lazy_import_rag()
    if rag_import_error:
        raise HTTPException(
            status_code=500,
            detail=f"RAG init failed: {rag_import_error}"
        )

    msg = (req.message or "").strip()
    if not msg:
        raise HTTPException(status_code=400, detail="message is empty")

    try:
        res = rag_ask(RAGAsk(question=msg))
        answer = (res.get("answer") or "").strip()
        if not answer:
            answer = "Mình không tìm thấy thông tin trong lịch tuần này."
        return ChatResponse(answer=answer)
    except Exception as e:
        raise HTTPException(500, detail=f"internal_error: {e}")

# ====== Legacy /ask (optional) ======
class AskIn(BaseModel):
    question: str

@router.post("/ask")
def api_ask_compat(req: AskIn):
    _lazy_import_rag()
    if rag_import_error:
        raise HTTPException(500, detail=f"RAG init failed: {rag_import_error}")
    try:
        return rag_ask(RAGAsk(question=req.question))
    except Exception as e:
        raise HTTPException(500, detail=f"internal_error: {e}")