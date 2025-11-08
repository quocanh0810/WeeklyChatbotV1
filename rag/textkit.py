# rag/textkit.py
from __future__ import annotations
import re
from typing import Dict, List, Optional

# ===================== Regex & parsing =====================
RE_DDMMYYYY = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")
RE_DDMM     = re.compile(r"\b(\d{1,2})[/-](\d{1,2})\b")
RE_DOW = re.compile(
    r"\b(t(h(ứ|u))\s*(2|3|4|5|6|7|hai|ba|tư|nam|năm|sáu|sau|bảy)|t[2-7]|chủ nhật|chu nhat|cn)\b",
    re.IGNORECASE
)
RE_WEEK = re.compile(r"\b(lịch\s+toàn\s+tuần|toàn\s+tuần)\b", re.IGNORECASE)
RE_DEFINE   = re.compile(r"\b(là gì|là cái gì|what\s+is)\b", re.IGNORECASE)
RE_TIME     = re.compile(r"\b(\d{1,2})(?:(?::|[hH])\s?(\d{2})?)\b")
RE_CALENDAR_HINT = re.compile(
    r"\b(lịch|họp|công tác|sự kiện|kế hoạch|khai giảng|xét tuyển|hội đồng|hôm nay|tuần này|ngày mai|thứ|ngày|giờ|địa điểm)\b",
    re.IGNORECASE,
)
RE_SMALLTALK = re.compile(
    r"\b(xin chào|chào|hello|hi|bạn là ai|giới thiệu|tên bạn|làm công việc gì|what do you do|help|giúp)\b",
    re.IGNORECASE,
)

# Hỏi ngày hôm nay (hoặc today)
RE_TODAY = re.compile(
    r"(h[oô]m\s*nay|today)", re.IGNORECASE
)


def _time_to_int(t: str) -> int:
    h, m = t.split(":")
    return int(h) * 60 + int(m)

def parse_times(query: str) -> tuple[Optional[str], Optional[str]]:
    matches: List[str] = []
    for m in RE_TIME.finditer(query):
        prev = query[max(0, m.start()-6):m.start()].lower()
        if re.search(r"thứ\s*$", prev):  # tránh nhầm "Thứ 5" -> 05:00
            continue
        h, mm = m.group(1), m.group(2) or "00"
        matches.append(f"{int(h):02d}:{int(mm):02d}")
    if not matches: return (None, None)
    if len(matches) == 1: return (matches[0], None)
    matches.sort()
    return (matches[0], matches[-1])

def filter_events_by_time(events: List[Dict], t_from: str, t_to: Optional[str] = None, tolerance_min: int = 5) -> List[Dict]:
    tf = _time_to_int(t_from)
    tt = _time_to_int(t_to) if t_to else None
    out: List[Dict] = []
    for ev in events:
        s = ev.get("start"); e = ev.get("end")
        if not s: continue
        si = _time_to_int(s); ei = _time_to_int(e) if e else si
        if tt is None:
            if abs(si - tf) <= tolerance_min or (si <= tf <= ei):
                out.append(ev)
        else:
            if not e:
                if tf <= si <= tt: out.append(ev)
            else:
                if max(si, tf) <= min(ei, tt): out.append(ev)
    return out

# ===================== KB & DOW normalization =====================
TMU_WEEKLY_KB = {
    "definition": (
        "“Lịch tuần” của Trường Đại học Thương Mại (TMU) là văn bản/tập tin tổng hợp "
        "các cuộc họp, sự kiện và công việc trong một tuần, kèm ngày, giờ, địa điểm, thành phần tham dự."
    ),
    "functions": [
        "Thông báo kế hoạch công tác tuần cho BGH, đơn vị và cá nhân liên quan.",
        "Điều phối phòng họp, nguồn lực và phân công tham dự (tránh trùng lịch).",
        "Làm căn cứ truyền thông nội bộ/đối ngoại cho các sự kiện của Trường.",
        "Lưu vết kế hoạch để đối chiếu, tổng kết công tác."
    ],
    "closing": "Bạn cần mình tra thử một ngày/thứ cụ thể trong lịch tuần hiện tại không?",
}

GENERAL_PERSONA = (
    "Bạn là trợ lý thân thiện của Trường Đại học Thương mại (TMU). "
    "Trả lời NGẮN GỌN (1–3 câu), tự nhiên, lịch sự, rõ ý. "
    "Nếu câu hỏi ngoài phạm vi, hãy nói thẳng và gợi ý người dùng hỏi về lịch công tác."
)

SMALLTALK_TEMPLATES = {
    "who": "Mình là trợ lý lịch công tác của TMU. Mình có thể tìm lịch theo ngày, thứ hoặc từ khóa.",
    "what_do": "Mình hỗ trợ tra cứu lịch tuần, tóm tắt cuộc họp/sự kiện theo ngày hay chủ đề.",
    "help": "Bạn có thể hỏi: “Thứ 5 có gì?”, “20/08/2025 họp gì?”, hoặc “các hoạt động về EMBA”.",
}

_DOW_ALIASES = {
    "thu2": {"thu2", "thứ 2", "thứ hai", "t2", "thu hai", "thu 2"},
    "thu3": {"thu3", "thứ 3", "thứ ba", "t3", "thu ba", "thu 3"},
    "thu4": {"thu4", "thứ 4", "thứ tư", "t4", "thu tu", "thu 4"},
    "thu5": {"thu5", "thứ 5", "thứ năm", "t5", "thu nam", "thu 5"},
    "thu6": {"thu6", "thứ 6", "thứ sáu", "t6", "thu sau", "thu 6"},
    "thu7": {"thu7", "thứ 7", "thứ bảy", "t7", "thu bay", "thu 7"},
    "cn":   {"cn", "chủ nhật", "chu nhat", "chủ nhật"},
}

_DOW_MAP = {
    "thứ 2": ["thứ hai", "thu hai", "th2", "t2", "thứ 2"],
    "thứ 3": ["thứ ba", "thu ba", "th3", "t3", "thứ 3"],
    "thứ 4": ["thứ tư", "thu tu", "th4", "t4", "thứ 4"],
    "thứ 5": ["thứ năm", "thu nam", "th5", "t5", "thứ 5"],
    "thứ 6": ["thứ sáu", "thu sau", "th6", "t6", "thứ 6"],
    "thứ 7": ["thứ bảy", "thu bay", "th7", "t7", "thứ 7"],
    "chủ nhật": ["chủ nhật", "chu nhat", "cn"],
}

def _canon_dow(s: str) -> str:
    q = re.sub(r"\s+", " ", (s or "").strip().lower())
    for canon, variants in _DOW_MAP.items():
        if any(q == v for v in variants) or any(v in q for v in variants):
            return canon
    return q

# ===================== Formatters =====================
def _format_event_lines(events: list[dict]) -> list[str]:
    out_blocks: list[str] = []
    evs = sorted(events, key=lambda ev: (ev.get("start") or "99:99", ev.get("id") or 0))
    for ev in evs:
        start = (ev.get("start") or "Cả ngày").strip()
        loc   = (ev.get("location") or "").strip()
        title = (ev.get("title") or "").strip()
        part  = (ev.get("participants") or "").strip()

        title = re.sub(r"\bTP[:\-]?\s*", "", title, flags=re.IGNORECASE)
        part_clean = re.sub(r"^\s*TP[:\-]?\s*", "", part, flags=re.IGNORECASE).strip()

        block_lines = []
        if loc and loc.lower() not in title.lower():
            block_lines.append(f"- **{start}** tại **{loc}**: {title}")
        else:
            block_lines.append(f"- **{start}**: {title}")
        if part_clean:
            block_lines.append(f"  - **Thành phần:** {part_clean}")

        out_blocks.append("\n".join(block_lines))
    return out_blocks

def format_events_full(events: list[dict]) -> str:
    if not events:
        return "Mình không tìm thấy thông tin trong lịch tuần này"
    d, dw = events[0].get("date"), events[0].get("dow")
    intro = f"Chào bạn! Mình vừa tra lịch và thấy các hoạt động vào **{d}, {dw}** như sau:\n"
    body  = "\n\n".join(_format_event_lines(events))
    outro = "\n\nBạn muốn mình lọc theo đơn vị/khung giờ khác, hoặc kiểm tra ngày khác không?"
    return intro + body + outro

def format_events_time_in_day(events, date_str, dow, t_from, t_to):
    pretty = f"**{t_from}**" if not t_to else f"**{t_from}–{t_to}**"
    if not events:
        return f"Mình đã kiểm tra **{date_str}{', ' + dow if dow else ''}** nhưng không thấy hoạt động đúng vào khung giờ {pretty}."
    intro = f"Đây là các hoạt động **{date_str}{', ' + dow if dow else ''}** trùng với {pretty}:\n"
    body  = "\n\n".join(_format_event_lines(events))
    outro = "\n\nCần mình xem các giờ lân cận không?"
    return intro + body + outro

def format_events_by_time_across_week(grouped, t_from, t_to):
    pretty = f"**{t_from}**" if not t_to else f"**{t_from}–{t_to}**"
    if not grouped:
        return f"Mình đã rà cả tuần nhưng không thấy hoạt động nào đúng vào {pretty}."
    parts = [f"Mình vừa lọc các hoạt động trong tuần theo khung giờ {pretty}:\n"]
    for date_str in sorted(grouped.keys(), key=lambda d: tuple(map(int, d.split('/')[::-1]))):
        evs = grouped[date_str]; dw = evs[0].get("dow")
        parts.append(f"\n**{date_str}, {dw}:**")
        parts.append("\n\n".join(_format_event_lines(evs)))
    parts.append("\n\nBạn muốn mình xem ngày/đơn vị khác không?")
    return "\n".join(parts)