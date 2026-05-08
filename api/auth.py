from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from config import JWT_TOKEN

bearer = HTTPBearer()


def require_auth(credentials: HTTPAuthorizationCredentials = Security(bearer)):
    if credentials.credentials != JWT_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.credentials
