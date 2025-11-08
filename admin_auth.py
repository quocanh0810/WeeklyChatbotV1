
import os, time, hmac, hashlib, base64
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SECRET = os.getenv("ADMIN_SECRET", "dev-secret-change-me")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "changeme")

security = HTTPBearer()

def _sign(s: bytes)->str:
    return base64.urlsafe_b64encode(hmac.new(SECRET.encode(), s, hashlib.sha256).digest()).decode()

def make_token(user: str, exp_sec: int=3600) -> str:
    payload = f"{user}|{int(time.time())+exp_sec}".encode()
    sig = _sign(payload)
    return base64.urlsafe_b64encode(payload).decode()+"."+sig

def verify_token(token: str)->str:
    try:
        payload_b64, sig = token.split(".")
        payload = base64.urlsafe_b64decode(payload_b64.encode())
        if _sign(payload) != sig: raise ValueError("bad-sign")
        user, exp = payload.decode().split("|")
        if int(exp) < int(time.time()): raise ValueError("expired")
        return user
    except Exception:
        raise HTTPException(401, "Invalid token")

def require_admin(creds: HTTPAuthorizationCredentials = Depends(security)):
    user = verify_token(creds.credentials)
    if user != ADMIN_USER:
        raise HTTPException(403, "Forbidden")
    return user