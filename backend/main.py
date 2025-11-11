# backend/main.py
from __future__ import annotations
import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

# =====================
# Env & Path resolution
# =====================
load_dotenv()

# project_root/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONT_USER   = PROJECT_ROOT / "frontend" / "user"
FRONT_ADMIN  = PROJECT_ROOT / "frontend" / "admin"

app = FastAPI(title="TMU Weekly Bot", version="1.0.0")

# ===== CORS =====
ALLOW_ORIGINS = [
    o.strip() for o in os.getenv("ALLOW_ORIGINS", "*").split(",") if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================
# Mount User Frontend
# ==================
# Phục vụ toàn bộ file tĩnh của user UI tại /assets
if FRONT_USER.exists():
    app.mount("/assets", StaticFiles(directory=FRONT_USER, html=False), name="user-assets")

# Luôn có route "/" để tránh 404 khi UI chưa sẵn
@app.get("/")
def user_index():
    idx = FRONT_USER / "index.html"
    if idx.exists():
        return FileResponse(idx)
    return {
        "message": "User UI chưa sẵn. Hãy đặt file vào frontend/user/index.html (kèm main.js, style.css)."
    }

# Favicon (tránh spam 404 trong log)
@app.get("/favicon.ico")
def favicon():
    fav = FRONT_USER / "favicon.ico"
    if fav.exists():
        return FileResponse(fav)
    return Response(status_code=204)

# ====================
# Mount Admin Frontend
# ====================
# Mount cả thư mục admin tại /admin (đường dẫn tương đối trong admin/index.html sẽ trở thành /admin/...)
if FRONT_ADMIN.exists():
    app.mount("/admin", StaticFiles(directory=FRONT_ADMIN, html=True), name="admin-ui")

# ===============
# Include API Routers
# ===============
# /api/chat
try:
    from backend.api.user_api import router as user_router
    app.include_router(user_router)
except Exception:
    pass

# /api/admin/*
try:
    from backend.api.admin_api import router as admin_router
    app.include_router(admin_router)
except Exception:
    pass

# =================
# Health & Version
# =================
@app.get("/health")
def health():
    return {"status": "ok", "version": app.version}

@app.get("/version")
def version():
    return {"name": "TMU Weekly Bot", "version": app.version}

# (Tùy chọn) Debug đường dẫn – tiện kiểm tra khi 404
@app.get("/_debug/paths")
def debug_paths():
    return {
        "project_root": str(PROJECT_ROOT),
        "front_user": str(FRONT_USER),
        "front_user_exists": FRONT_USER.exists(),
        "front_admin": str(FRONT_ADMIN),
        "front_admin_exists": FRONT_ADMIN.exists(),
        "allow_origins": ALLOW_ORIGINS,
    }