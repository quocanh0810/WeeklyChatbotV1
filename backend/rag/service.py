# rag/service.py
from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel, Field, validator
from datetime import datetime, timedelta
from google import genai

from .settings import GEMINI_API_KEY, GEMINI_MODEL
from .io_store import (
    get_events_by_date,
    list_all_dates,
    vector_search,
    _fetch_all_date_dow_pairs,
)
from .textkit import (
    TMU_WEEKLY_KB,
    GENERAL_PERSONA,
    SMALLTALK_TEMPLATES,
    RE_DDMMYYYY,
    RE_DDMM,
    RE_DOW,
    RE_WEEK,
    RE_DEFINE,
    RE_CALENDAR_HINT,
    RE_SMALLTALK,
    parse_times,
    filter_events_by_time,
    format_events_full,
    format_events_time_in_day,
    format_events_by_time_across_week,
    _canon_dow,
)

# LLM client
gclient = genai.Client(api_key=GEMINI_API_KEY)

SYSTEM_PROMPT = (
    "Bạn là trợ lý lịch công tác. Trả lời BẰNG TIẾNG VIỆT và CHỈ dựa trên ngữ cảnh cung cấp. "
    "Nếu không đủ thông tin, hãy trả lời: 'Mình không tìm thấy thông tin trong lịch tuần này'. "
    "Luôn nêu rõ NGÀY, THỨ, GIỜ, ĐỊA ĐIỂM, THÀNH PHẦN nếu có."
)

# Helpers
VI_DOW = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ nhật"]  # datetime.weekday(): 0..6

def _fmt_vi_date(d: datetime) -> tuple[str, str]:
    """Trả về (dd/mm/yyyy, 'Thứ x' hoặc 'Chủ nhật')."""
    return d.strftime("%d/%m/%Y"), VI_DOW[d.weekday()]

def _is_today_question(q: str) -> bool:
    ql = q.lower()
    return ("hôm nay" in ql or "today" in ql) and any(k in ql for k in ["ngày", "date", "mấy", "bao nhiêu", "thứ"])

def _is_tomorrow_question(q: str) -> bool:
    ql = q.lower()
    return ("ngày mai" in ql or "tomorrow" in ql) and any(k in ql for k in ["ngày", "date", "mấy", "thứ"])

# intent
def classify_intent(q: str) -> str:
    qn = (q or "").strip().lower()
    if not qn:
        return "GENERAL"
    if RE_SMALLTALK.search(qn):
        return "SMALLTALK"
    if RE_DEFINE.search(qn):
        return "DEFINE"
    # có ngày/tháng/năm hoặc thứ → SCHEDULE
    has_date = bool(RE_DDMMYYYY.search(qn) or RE_DDMM.search(qn) or RE_DOW.search(qn))
    if has_date:
        return "SCHEDULE"
    if RE_WEEK.search(qn):
        return "SCHEDULE_ALL"
    if RE_CALENDAR_HINT.search(qn):
        return "SCHEDULE"
    return "GENERAL"

def _smalltalk_reply(q: str) -> str:
    ql = q.lower()
    if "bạn là ai" in ql or "who" in ql:
        return SMALLTALK_TEMPLATES["who"]
    if "làm công việc gì" in ql:
        return SMALLTALK_TEMPLATES["what_do"]
    if "help" in ql or "giúp" in ql:
        return SMALLTALK_TEMPLATES["help"]
    return "Chào bạn! Mình là trợ lý lịch công tác của TMU. Bạn cần mình kiểm tra ngày/thứ nào không?"

def _general_reply(q: str) -> str:
    prompt = f"{GENERAL_PERSONA}\n\n[Người dùng]: {q}\n[Trợ lý]:"
    resp = gclient.models.generate_content(model=GEMINI_MODEL, contents=prompt)
    text = getattr(resp, "text", None) or getattr(resp, "output_text", None)
    if text:
        return text.strip()
    try:
        return resp.candidates[0].content.parts[0].text.strip()
    except Exception:
        pass
    try:
        return resp.candidates[0].content[0].text.strip()
    except Exception:
        pass
    return "Mình chưa chắc câu này. Bạn có thể hỏi lại ngắn gọn hơn không?"

# LLM prompt builder
def build_prompt(question: str, contexts: List[Dict]) -> str:
    header = SYSTEM_PROMPT + "\n\n[CÁC ĐOẠN LIÊN QUAN]\n"
    ctx = ""
    for i, c in enumerate(contexts, 1):
        meta = []
        if c.get("date"):
            meta.append(f"Ngày: {c['date']}")
        if c.get("dow"):
            meta.append(f"Thứ: {c['dow']}")
        if c.get("start"):
            meta.append(f"Giờ: {c['start']}")
        if c.get("location"):
            meta.append(f"Địa điểm: {c['location']}")
        if c.get("participants"):
            meta.append(f"TP: {c['participants']}")
        score = c.get("score")
        head = f"\n--- ĐOẠN {i}" + (f" (score={score:.3f})" if score is not None else "") + " ---\n"
        ctx += head + (" | ".join(meta)) + "\n" + (c.get("text") or "") + "\n"
    user = f"\n[CÂU HỎI]\n{question}\n\n[HƯỚNG DẪN]\nNếu nhiều sự kiện cùng ngày, hãy liệt kê TẤT CẢ."
    return header + ctx + user

def call_gemini(prompt: str) -> str:
    resp = gclient.models.generate_content(model=GEMINI_MODEL, contents=prompt)
    text = getattr(resp, "text", None) or getattr(resp, "output_text", None)
    if text:
        return text.strip()
    try:
        return resp.candidates[0].content.parts[0].text.strip()
    except Exception:
        pass
    try:
        return resp.candidates[0].content[0].text.strip()
    except Exception:
        pass
    return ""

# pydantic I/O
class Ask(BaseModel):
    question: str = Field(..., description="Câu hỏi người dùng")

    @validator("question")
    def _strip(cls, v: str) -> str:
        return (v or "").strip()

# main service
def ask(payload: Ask):
    q = (payload.question or "").strip()
    t_from, t_to = parse_times(q)

    # Câu hỏi “HÔM NAY/NGÀY MAI là ngày bao nhiêu/thứ mấy?”
    if _is_today_question(q):
        today = datetime.now()
        date_str, dow = _fmt_vi_date(today)

        # Map sang date có trong DB (nếu tuần đang nạp có chứa ngày hôm nay)
        # Nếu không có, vẫn trả lời ngày/thứ cho người dùng.
        events = get_events_by_date(date_str)
        if events:
            return {"answer": format_events_full(events), "hits": events}
        return {"answer": f"Hôm nay là **{date_str}, {dow}**. Mình chưa thấy lịch ngày này trong dữ liệu tuần đang có.", "hits": []}

    if _is_tomorrow_question(q):
        tomorrow = datetime.now() + timedelta(days=1)
        date_str, dow = _fmt_vi_date(tomorrow)
        events = get_events_by_date(date_str)
        if events:
            return {"answer": format_events_full(events), "hits": events}
        return {"answer": f"Ngày mai là **{date_str}, {dow}**. Mình chưa thấy lịch ngày này trong dữ liệu tuần đang có.", "hits": []}

    # Phân loại intent còn lại
    intent = classify_intent(q)

    # DEFINE
    if intent == "DEFINE":
        ql = q.lower()
        if "lịch tuần" in ql or "weekly" in ql:
            if any(k in ql for k in ["chức năng", "mục đích", "tác dụng", "role", "function"]):
                bullets = "\n".join([f"- {it}" for it in TMU_WEEKLY_KB["functions"]])
                ans = f"{TMU_WEEKLY_KB['definition']}\n\n**Chức năng chính:**\n{bullets}\n\n{TMU_WEEKLY_KB['closing']}"
                return {"answer": ans, "hits": []}
            return {"answer": f"{TMU_WEEKLY_KB['definition']}\n\n{TMU_WEEKLY_KB['closing']}", "hits": []}
        return {"answer": _general_reply(q), "hits": []}

    if intent == "SMALLTALK":
        return {"answer": _smalltalk_reply(q), "hits": []}

    if intent == "GENERAL":
        return {"answer": _general_reply(q), "hits": []}

    # SCHEDULE_ALL
    if intent == "SCHEDULE_ALL":
        dates = list_all_dates()
        if not dates:
            return {"answer": "Mình không tìm thấy thông tin trong lịch tuần này.", "hits": []}
        answers, all_hits = [], []
        for ds in dates:
            evs = get_events_by_date(ds)
            if evs:
                answers.append(format_events_full(evs))
                all_hits.extend(evs)
        final = "Mình vừa tổng hợp lịch công tác của toàn bộ tuần:\n\n" + "\n\n".join(answers)
        return {"answer": final, "hits": all_hits}

    # SCHEDULE (theo ngày/giờ)
    # dd/mm/yyyy
    m = RE_DDMMYYYY.search(q)
    if m:
        date_str = f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{int(m.group(3)):04d}"
        events = get_events_by_date(date_str)
        if not events:
            return {"answer": f"Mình không tìm thấy hoạt động nào vào {date_str}.", "hits": []}
        if t_from:
            filtered = filter_events_by_time(events, t_from, t_to)
            return {
                "answer": format_events_time_in_day(filtered, date_str, events[0]["dow"], t_from, t_to),
                "hits": filtered,
            }
        return {"answer": format_events_full(events), "hits": events}

    # dd/mm
    m2 = RE_DDMM.search(q)
    if m2 and not m:
        d, mth = int(m2.group(1)), int(m2.group(2))
        for ds in list_all_dates():
            dd, mm, _yy = ds.split("/")
            if int(dd) == d and int(mm) == mth:
                events = get_events_by_date(ds)
                if events:
                    if t_from:
                        filtered = filter_events_by_time(events, t_from, t_to)
                        return {
                            "answer": format_events_time_in_day(filtered, ds, events[0]["dow"], t_from, t_to),
                            "hits": filtered,
                        }
                    return {"answer": format_events_full(events), "hits": events}

    # Thứ ...
    mdow = RE_DOW.search(q)
    if mdow:
        canon_q = _canon_dow(mdow.group(0))
        for d, dw in _fetch_all_date_dow_pairs():
            if _canon_dow(dw) == canon_q or canon_q in _canon_dow(dw):
                date_str = d
                events = get_events_by_date(date_str)
                if not events:
                    return {"answer": f"Mình không tìm thấy hoạt động nào vào {mdow.group(0)}.", "hits": []}
                if t_from:
                    filtered = filter_events_by_time(events, t_from, t_to)
                    return {
                        "answer": format_events_time_in_day(filtered, date_str, events[0]["dow"], t_from, t_to),
                        "hits": filtered,
                    }
                return {"answer": format_events_full(events), "hits": events}

    # Chỉ có giờ -> quét cả tuần
    if t_from and not (m or m2 or mdow):
        grouped, all_hits = {}, []
        for ds in list_all_dates():
            evs = get_events_by_date(ds)
            hit = filter_events_by_time(evs, t_from, t_to)
            if hit:
                grouped[ds] = hit
                all_hits.extend(hit)
        return {"answer": format_events_by_time_across_week(grouped, t_from, t_to), "hits": all_hits}

    # Fallback: RAG + LLM
    hits = vector_search(q, k=20)
    prompt = build_prompt(q, hits)
    txt = call_gemini(prompt).strip()
    wrapped = (
        "Mình vừa xem trong lịch tuần và tổng hợp được như sau:\n\n"
        + txt
        + "\n\nBạn cần mình kiểm tra thêm ngày/đơn vị khác không?"
        if txt
        else "Mình không tìm thấy thông tin trong lịch tuần này."
    )
    return {"answer": wrapped, "hits": hits}