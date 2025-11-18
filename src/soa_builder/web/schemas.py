from pydantic import BaseModel
from typing import Optional


class ArmCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    data_origin_type: Optional[str] = None


class ArmUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    data_origin_type: Optional[str] = None
