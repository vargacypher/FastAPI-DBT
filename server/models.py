from pydantic import BaseModel, Field


class StatusResponse(BaseModel):
    success:  bool
