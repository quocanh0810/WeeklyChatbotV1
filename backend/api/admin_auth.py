# backend/api/admin_auth.py
import os, time, hmac, base64
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# --- Config từ ENV ---
SECRET = os.getenv("ADMIN_SECRET", "dev-secret-change-me")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "changeme")

# Chuẩn hoá secret -> bytes & sanity check
try:
    SECRET_BYTES = SECRET.encode("utf-8")
except Exception:
    raise RuntimeError("ADMIN_SECRET must be valid UTF-8")

if len(SECRET_BYTES) < 16:
    # tuỳ bạn: có thể raise cứng ở prod
    # raise RuntimeError("ADMIN_SECRET too weak (<16 bytes)")
    pass

# Nếu muốn nới lỏng cho swagger thử nghiệm thì auto_error=False
security = HTTPBearer(auto_error=True)

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")

def _b64url_decode(s: str) -> bytes:
    # thêm padding nếu thiếu
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode())

def _sign(payload: bytes) -> str:
    dig = hmac.new(SECRET_BYTES, payload, digestmod="sha256").digest()
    return _b64url(dig)

def make_token(user: str, exp_sec: int = 3600) -> str:
    # token = base64url(payload) + "." + base64url(signature)
    # payload = "user|exp"
    if "|" in user:
        # tránh phá format payload
        raise ValueError("username must not contain '|'")
    exp = int(time.time()) + int(exp_sec)
    payload = f"{user}|{exp}".encode("utf-8")
    sig = _sign(payload)
    return f"{_b64url(payload)}.{sig}"

def verify_token(token: str) -> str:
    try:
        payload_b64, sig_given = token.strip().split(".", 1)
        payload = _b64url_decode(payload_b64)
        sig_calc = _sign(payload)
        # so sánh constant-time
        if not hmac.compare_digest(sig_calc, sig_given):
            raise ValueError("bad-sign")

        user, exp_str = payload.decode("utf-8").split("|", 1)
        if int(exp_str) < int(time.time()):
            raise ValueError("expired")
        return user
    except Exception:
        # để tránh lộ lý do cụ thể -> trả 401 chung
        raise HTTPException(status_code=401, detail="Invalid token")

def require_admin(creds: HTTPAuthorizationCredentials = Depends(security)) -> str:
    if not creds or not creds.scheme.lower() == "bearer":
        raise HTTPException(status_code=401, detail="Missing bearer token")
    user = verify_token(creds.credentials)
    if user != ADMIN_USER:
        raise HTTPException(status_code=403, detail="Forbidden")
    return user