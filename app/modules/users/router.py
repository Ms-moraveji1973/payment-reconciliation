from fastapi import APIRouter , Depends , status , HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .schema import UserSchema,UserResponseSchema , CursorResponse , CursorParams
from app.db.database import get_db
from .service import create_user_service , get_all_users


router = APIRouter(prefix="/users", tags=["users"])

@router.post("/create-user",response_model=UserResponseSchema,status_code=status.HTTP_201_CREATED)
async def create_user(user:UserSchema,session:AsyncSession = Depends(get_db)):
    try :
        new_user = await create_user_service(user,session)
        await session.commit()
        await session.refresh(new_user)
        return new_user
    except ValueError:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="User already exists")


@router.get("/get-users",response_model=CursorResponse[UserResponseSchema],status_code=status.HTTP_200_OK)
async def get_users(params: CursorParams = Depends(), session:AsyncSession = Depends(get_db)):
    users,next_cursor,has_more = await get_all_users(params.cursor,params.limit,session)
    return CursorResponse(
        items=users,
        next_cursor=next_cursor,
        has_more=has_more,
    )