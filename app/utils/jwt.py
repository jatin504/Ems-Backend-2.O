import jwt
import os
from datetime import datetime, timedelta

SECRET_KEY = os.getenv("JWT_SECRET", "AAA@$DH&*(Z)")
ALGORITHM = "HS256"

def create_access_token(data: dict, expires_delta: int = 64800):  # 18 hours in seconds
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(seconds=expires_delta)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Optional: Alias for create_access_token
def generate_token(data: dict, expires_delta: int = 64800):  # 18 hours in seconds
    return create_access_token(data, expires_delta)

def decode_access_token(token: str):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        return None
