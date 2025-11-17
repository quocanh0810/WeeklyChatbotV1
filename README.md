# Weekly Schedule Chatbot (Self-hosted RAG with FAISS + SQLite)

## Overview

Weekly Schedule Chatbot is a fully self-hosted RAG-based system for question-answering on weekly schedules.
Upload a .docx schedule → the system parses tables, chunks the content, generates embeddings, stores vectors in FAISS, metadata in SQLite, and uses Gemini Google to answer user queries with high precision.

---

## Technical Stack

| Component | Technology | Role |
|-----------|-----------|---------|
| Frontend | HTML / CSS / JavaScript | User & admin interface |
| Backend API | FastAPI (Python) | API server, ingestion, auth, schedule querying |
| RAG Engine | OpenAI Embeddings + FAISS | Vector store + Top-K similarity search |
| Metadata Store | SQLite | Chunk metadata, ingestion logs |
| Document Parser | python-docx + regex | Extract weekly tables from .docx files |
| Auth | Token-based (custom mini JWT) | Admin login & session handling |
| Storage | FAISS Index + SQLite + uploads/ | Persist vectors, metadata, and uploaded files | 

---

## Quick Setup

```bash
python -m venv .venv && source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

cp .env.example .env

# Run backend API

uvicorn backend.main:app --reload --port 8000

# Admin credentials (add to .env):
ADMIN_USER=...
ADMIN_PASS=...
``` 

![Weekly Chatbot Screenshot](./doc/OverviewModel.png)
