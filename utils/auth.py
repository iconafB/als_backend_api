from fastapi import HTTPException,Depends,status
from sqlmodel import select
from fastapi.security import OAuth2PasswordBearer,HTTPBasic,HTTPBasicCredentials
from passlib.context import CryptContext
from typing import Annotated
from sqlalchemy.ext.asyncio.session import AsyncSession
from datetime import datetime,timedelta,timezone
import jwt
from jwt.exceptions import InvalidTokenError
import secrets
from settings.Settings import get_settings
from models.users import users_tbl
from database.master_database_prod import get_async_master_prod_session
from schemas.auth import TokenData
pwd_context=CryptContext(schemes=["bcrypt"],deprecated="auto")

security=HTTPBasic()
oauth_scheme=OAuth2PasswordBearer(tokenUrl='auth/login')
#hash password
def hash_password(password):
    return pwd_context.hash(password)

#verify password
def verify_password(plain_password,hashed_password):
    return pwd_context.verify(plain_password,hashed_password)

#create access token
def create_access_token(data:dict,expires_delta:timedelta | None=None):
    to_encode=data.copy()
    #create expires_dalta
    if expires_delta:
        expire=datetime.now(timezone.utc) + expires_delta
    else:
        expire=datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({'exp':expire})
    encoded_jwt=jwt.encode(to_encode,get_settings().SECRET_KEY,algorithm=get_settings().ALGORITHM)
    return encoded_jwt

#verify access token
def verify_token(token:str,credentials_exception):
    try:
        payload=jwt.decode(token,get_settings().SECRET_KEY,algorithms=[get_settings().ALGORITHM])
        user_id:str=payload.get("user_id")
        if user_id is None:
            raise credentials_exception 
        return user_id
    except InvalidTokenError:
        raise credentials_exception
    


#get current user

async def get_current_user(token:Annotated[str,Depends(oauth_scheme)]):
    credential_exception=HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail=f"Could not validate credentials",headers={"WWW-Authenticate": "Bearer"})
    try:
        payload=jwt.decode(token,get_settings().SECRET_KEY,algorithms=[get_settings().ALGORITHM])
        user_id=payload['user_id']
        if user_id==None:
            raise credential_exception 
    except InvalidTokenError:
        raise credential_exception
    return user_id

async def get_current_active_user(current_user:Annotated[int,Depends(get_current_user)],session:AsyncSession=Depends(get_async_master_prod_session)):
    credential_exception=HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail=f"Invalid credentials",headers={"WWW-Authenticate": "Bearer"})
    if current_user==None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Invalid Credentials")
    user_query=select(users_tbl).where(users_tbl.id==current_user)
    #exceute the query and get user data
    result_user=await session.exec(user_query)
    user=result_user.first()
    #raise an exception if the user does not exist
    if user is None:
        raise credential_exception
    return user


def require_docs_auth(creds:HTTPBasicCredentials=Depends(security)):
    settings=get_settings()
    user_ok=secrets.compare_digest(creds.username,settings.ADMIN_USERNAME)
    pass_ok=secrets.compare_digest(creds.password,settings.ADMIN_PASSWORD)
    if not (user_ok and pass_ok):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail=f"Unauthorized",headers={"WWW-Authenticate":"Basic"})
    
    return True
