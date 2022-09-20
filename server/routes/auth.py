import os
from fastapi import APIRouter,Depends,HTTPException
from utils.jwt_verify import create_access_token,has_access
from datetime import timedelta

from starlette.config import Config
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

from authlib.integrations.starlette_client import OAuth
from authlib.integrations.starlette_client import OAuthError

config = Config('.env')
oauth = OAuth(config)

CONF_URL = 'https://accounts.google.com/.well-known/openid-configuration'
oauth.register(
    name='google',
    server_metadata_url=CONF_URL,
    client_kwargs={
        'scope': 'openid email profile'
    }
)

router = APIRouter()


@router.get("/airflow/login", tags=["Login"])
async def read_users(token: str = Depends(has_access)):
    return [{"hello": "airflow"}]


@router.get('/login', tags=['Login'])  # Tag it as "authentication" for our docs
async def login(request: Request):
    # Redirect Google OAuth back to our application
    redirect_uri = request.url_for('auth')

    return await oauth.google.authorize_redirect(request, redirect_uri)


@router.route('/auth')
async def auth(request: Request):
    # Perform Google OAuth
    try:
        access_token = await oauth.google.authorize_access_token(request)
        email = access_token.get('userinfo').get('email')

    except OAuthError:
        raise HTTPException(
            status_code=401,
            detail='Could not validate credentials',
            headers={'WWW-Authenticate': 'Bearer'},
        )
    # Save the user
    user = {"email":f"{email}"}

    #CHANGE TO HTTP_ONLY 
    request.session['token'] = create_access_token(user,timedelta(minutes=10))

    return RedirectResponse(url='/')


@router.get('/logout',tags=["Login"])
async def logout(request: Request):
    request.session.pop('token', None)
    return RedirectResponse(url='/')