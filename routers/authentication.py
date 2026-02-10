from fastapi import APIRouter,Depends,status,HTTPException,Request
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import select
from typing import Annotated
from sqlalchemy.ext.asyncio.session import AsyncSession
from datetime import timedelta
from models.users import users_tbl
from schemas.auth import RegisterUser,RegisterUserResponse,Token,GetUserResponse,CurrentlyLoggedInUser
from utils.auth import verify_password,get_current_active_user,create_access_token
from crud.users import (create_user,get_current_logged_in_user,forgot_password_db)
from settings.Settings import get_settings
from database.master_database_prod import get_async_master_prod_session
from utils.logger import define_logger

auth_logger=define_logger("als_auth_logger","logs/auth_route.log")

auth_router=APIRouter(tags=["Authentication"],prefix="/auth")
@auth_router.post("/register",status_code=status.HTTP_201_CREATED,response_model=RegisterUserResponse,description="Register user to the als by providing email,password, and full name")
async def register_user(user:RegisterUser,session:AsyncSession=Depends(get_async_master_prod_session)):
    return await create_user(user,session)

@auth_router.post("/login",status_code=status.HTTP_200_OK,response_model=Token,description="Login to the als by providing a password and email")
async def login_user(user:Annotated[OAuth2PasswordRequestForm,Depends()],session:AsyncSession=Depends(get_async_master_prod_session)):
    
    login_query=select(users_tbl).where(users_tbl.email==user.username)
    result=await session.execute(login_query)
    login_user=result.scalar_one_or_none()
    
    if login_user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"User not registered")
    #return an error if the user is not found
    if not login_user.email == user.username:
        auth_logger.info(f"user with email:{user.username} does not exist")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail="Invalid Credentials")
    #verify the password and return an error if the password is wrong
    if not verify_password(user.password,login_user.password):
        auth_logger.info(f"user password:{user.password} does not exist")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail="Invalid Credentials")
    #set the token expiration time
    access_token_expires=timedelta(minutes=get_settings().ACCESS_TOKEN_EXPIRES_MINUTES)
    #generate the access token
    token=create_access_token(data={'user_id':login_user.id},expires_delta=access_token_expires)
    #return the token
    auth_logger.info(f"username:{user.username} successfully logged in")
    return Token(access_token=token,token_type='Bearer')



@auth_router.get("/user",response_model=GetUserResponse)
async def get_the_current_user(current_user=Depends(get_current_active_user)):
    return current_user

@auth_router.get("/me",status_code=status.HTTP_200_OK,response_model=CurrentlyLoggedInUser)
async def get_currently_logged_in_user(session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await get_current_logged_in_user(session,user.id)

