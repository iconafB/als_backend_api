from fastapi import HTTPException,Depends,status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
from sqlmodel import select
from datetime import timedelta
from sqlalchemy.ext.asyncio.session import AsyncSession
from models.users import users_tbl
from schemas.auth import RegisterUser,RegisterUserResponse,Token,ForgotPassword,ForgotPasswordRequest,CurrentlyLoggedInUser
from utils.auth import verify_password,hash_password,create_access_token
from utils.logger import define_logger
from settings.Settings import get_settings
auth_logger=define_logger("als_auth_logs","logs/auth_route.log")

async def create_user(user:RegisterUser,session:AsyncSession)->RegisterUserResponse:
    try:
        user_query=select(users_tbl).where(users_tbl.email==user.email)
        user_resource=await session.execute(user_query)
        result=user_resource.one_or_none()
        if result is not None:
            auth_logger.info(f"user with email:{user.email} already exists")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"user with {user.email} already exist")
        user.password=hash_password(user.password)
        new_user=users_tbl(email=user.email,password=user.password,first_name=user.first_name,last_name=user.last_name,is_active=True,is_admin=user.is_admin)
        session.add(new_user)
        await session.commit()
        await session.refresh(new_user)
        auth_logger.info(f"user:{new_user.email} successfully registered")
        return new_user
    except HTTPException:
        raise 

    except Exception as e:
        auth_logger.exception(f"an internal server error occurred while registering user:{user.email}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred while registering a user")


async def login_user(user:Annotated[OAuth2PasswordRequestForm,Depends()],session:AsyncSession):
    try:
        login_query=select(users_tbl).where(users_tbl.email==user.username)
        logged_in_user=await session.execute(login_query)
        result=logged_in_user.one_or_none()
        if result is not None:
            auth_logger.info(f"user with:{user.username} does not exist")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail=f"Invalid credentials")
        if not verify_password(user.password,result.password):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail=f"Invalid credentials")
        
        access_token_expires=timedelta(minutes=get_settings().ACCESS_TOKEN_EXPIRES_MINUTES)
        token=create_access_token(data={'user_id':login_user.id},expires_delta=access_token_expires)
         #return the token
        auth_logger.info(f"username:{user.username} successfully logged in")
        return Token(access_token=token,token_type='Bearer')

    except HTTPException:
        raise

    except Exception as e:
        auth_logger.exception(f"An internal server error occured while logging user:{user.username}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while logging in user")




async def get_current_logged_in_user(session:AsyncSession,user:int)->CurrentlyLoggedInUser:
    user_query=select(users_tbl).where(users_tbl.id==user)
    user=await session.execute(user_query)
    result=user.scalar_one_or_none()
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"user:{user} does not exist")
    return CurrentlyLoggedInUser(user_id=result.id,email=result.email,first_name=result.first_name,last_name=result.last_name,is_admin=result.is_admin)


