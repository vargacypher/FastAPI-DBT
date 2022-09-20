import os
from jose import jwt
from jose.exceptions import JOSEError
from fastapi import HTTPException, Depends, Security,Request
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie
from datetime import datetime, timedelta

SECRET_KEY = '' #TODO env
ALGORITHM = "" #TODO env
API_KEY = ""
API_KEY_NAME = "access_token"

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(days=90)
        to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)

def has_access(request: Request,api_key_query: str = Security(api_key_query), token: str = ''):
    """
        Function that is used to validate the token in the case that it requires it
    """
    if api_key_query == API_KEY:
        pass
    else: 
        if request.session.get('token'):
            token = request.session.get('token')
            try:
                payload = jwt.decode(token, key=SECRET_KEY, options={"verify_signature": False,
                                                            "verify_aud": False,
                                                            "verify_iss": False})
            except JOSEError as e:  
                raise HTTPException(
                    status_code=401,
                    detail=str(e))