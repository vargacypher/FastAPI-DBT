import uvicorn
import logging
import json


from models import StatusResponse, Field
from fastapi import FastAPI, Response, Depends, HTTPException
from fastapi.responses import JSONResponse

from tasks import dbt_run, get_job_result, QUEUE, exceptions
from docs.openapi import custom_openapi as _custom_openapi

from routes import auth
from utils.jwt_verify import SECRET_KEY, has_access

from starlette.responses import HTMLResponse
from starlette.requests import Request

#Middlewares
from starlette.middleware.sessions import SessionMiddleware

#TODO
#ADD CORS

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Set custom OpenAPI docs.
def custom_openapi():
	return _custom_openapi(app)

app.include_router(auth.router)


@app.get('/')
async def home(request: Request):
	try:
		user = request.session.get('token',None)
		if user:
			html = (
				f'<pre>Email: {user}</pre><br>'
				'<a href="/docs">documentation</a><br>'
				'<a href="/logout">logout</a>'
			)
			return HTMLResponse(html)
	except:
		return HTMLResponse('<a href="/login">login</a>')


@app.post("/run", summary="RUN Endpoint Method", tags=["Methods API"])
async def run(models_path: str, vars_param: str):
    """  Executa os arquivos de <i>models</i> SQL compilados no banco de dados <i>target</i>. """
    job = dbt_run.delay(models_path, vars_param)
    response = {"dbt_method": "run",
                "job_id": job.id,
                "models_path": models_path,
                "vars_param": vars_param
                }

    return JSONResponse(response)


@app.get("/health", response_model=StatusResponse, tags=["Health Check"])
async def status():
    """ Define o status de disponibilidade do serviço. """
    return JSONResponse({"success": True})


@app.get("/jobs/result/{job_id}", tags=["Tasks"])
async def jobs_result(job_id: str):
    try:
        return JSONResponse({job_id:get_job_result(job_id)})
    except Exception as e:
        if isinstance(e, exceptions.NoSuchJobError):
            return JSONResponse({"description": "job not found",
                "id": job_id}, status_code=404)
        else:
            raise e

app.openapi = custom_openapi

