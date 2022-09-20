from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi


def custom_openapi(app: FastAPI):
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
            title="DBT Service",
            version="1.0",
            description="""<h3>Esta é API de serviço do DBT. </h3>""",
            routes=app.routes,
            )
    
    openapi_schema["info"]["x-logo"] = {
            "url": "https://pmweblabs.s3.amazonaws.com/assets/oto-logo.png"
                        }
    app.openapi_schema = openapi_schema
    return app.openapi_schema
