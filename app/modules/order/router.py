from fastapi import APIRouter

# internal
from .service import calculate_age

router = APIRouter(prefix="/order", tags=["order"])

@router.get("/")
async def hello_world():
    return {'message': 'OK'}


@router.get("/{age}")
async def test_get_order(age:int):
    return await calculate_age(age)

