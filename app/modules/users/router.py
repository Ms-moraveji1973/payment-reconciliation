from fastapi import APIRouter , Depends , status , HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .schema import UserSchema,UserResponseSchema
from app.db.database import get_db
from .service import create_user_service , get_all_users


router = APIRouter(prefix="/users", tags=["users"])

@router.post("/create_user",response_model=UserResponseSchema,status_code=200)
async def create_user(user:UserSchema,db:AsyncSession = Depends(get_db)):
    try :
        test = await create_user_service(user,db)
        return test
    except ValueError as v:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail=str(v))


@router.get("/get_users")
async def get_users(db:AsyncSession = Depends(get_db)):
    try :
        users = await get_all_users(db)
        return users
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_204_NO_CONTENT,detail=str(e))




