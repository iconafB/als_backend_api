from pydantic import BaseModel,EmailStr
from sqlmodel import SQLModel,Field
from typing import Optional

class RegisterUser(BaseModel):
    email:EmailStr
    password:str
    first_name:str
    last_name:str
    is_admin:bool=False


class GetUserResponse(BaseModel):
    email:str
    first_name:str
    last_name:str
    is_active:bool


class RegisterUserResponse(SQLModel):
    id:int
    first_name:str
    last_name:str
    email:EmailStr

class ForgotPassword(BaseModel):
    email:EmailStr
    new_password:str

class ForgotPasswordRequest(BaseModel):
    email:EmailStr


class LoginUser(BaseModel):
    email:EmailStr
    password:str


class Token(BaseModel):
    access_token:str
    token_type:str

class TokenData(BaseModel):
    username:int | None=None


class CurrentlyLoggedInUser(BaseModel):
    user_id:int
    email:EmailStr
    first_name:str
    last_name:str
    is_admin:bool


#password reset schemas

class PasswordResetRequestIn(BaseModel):
    email:EmailStr

#password reset confirm

class PasswordResetConfirmIn(BaseModel):
    token:str=Field(min_length=10)
    new_password:str=Field(min_length=10, max_length=128)
    