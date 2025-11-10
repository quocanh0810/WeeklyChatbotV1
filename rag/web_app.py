# rag/web_app.py
import os, traceback
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from admin_api import router as admin_router

app = FastAPI(title="TMU Weekly Bot", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount frontend chatbot
WEB_DIR = os.path.join(os.path.dirname(__file__), "..", "web")
WEB_DIR = os.path.abspath(WEB_DIR)
os.makedirs(WEB_DIR, exist_ok=True)
app.mount("/assets", StaticFiles(directory=WEB_DIR, html=False), name="assets")

@app.get("/")
def serve_index():
    index_path = os.path.join(WEB_DIR, "index.html")
    if not os.path.exists(index_path):
        return {"message": "Put your frontend in ./web (index.html, main.js, style.css)."}
    return FileResponse(index_path)

# Mount admin UI (static)
ADMIN_UI_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web_admin"))
if os.path.isdir(ADMIN_UI_DIR):
    app.mount("/admin", StaticFiles(directory=ADMIN_UI_DIR, html=True), name="admin-ui")

# Include admin API
app.include_router(admin_router)