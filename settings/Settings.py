from pydantic_settings import BaseSettings,SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    #master db information
    MASTER_DB_HOST_NAME:str
    MASTER_DB_PORT:int
    MASTER_DB_NAME:str
    MASTER_DB_USER:str
    MASTER_DB_PASSWORD:str
    MASTER_DB_OWNER:str
    #authentication details
    SECRET_KEY:str
    ALGORITHM:str
    ACCESS_TOKEN_EXPIRES_MINUTES:int
    #dmasa environment variables
    DMASA_API_KEY:str
    DMASA_MEMBER_ID:str
    UPLOAD_DMASA_URL:str
    READ_DMASA_DEDUPE_STATUS:str
    NOTIFICATION_EMAIL:str
    CHECK_CREDITS_DMASA_URL:str
    READ_DMASA_OUTPUT_URL:str
    #dedago environment variables
    dedago_url:str
    INVTNTDBN_TOKEN:str
    P3_TOKEN:str
    HQ_TOKEN:str
    YORK_TOKEN:str
    #pings environment variables
    hopper_level_check_url:str
    icon_ping_url:str
    send_pings_to_kuda_username:str
    send_pings_to_kuda_password:str
    send_pings_to_troy_url:str
    send_pings_to_troy_token:str
    #pings database
    pings_db_name:str
    pings_db_user:str
    pings_db_password:str
    pings_db_port:str
    pings_db_host:str

    #mail server 
    #API CREDENTIALS
    ADMIN_USERNAME:str
    ADMIN_PASSWORD:str
    
    #load the environment variables file
    model_config=SettingsConfigDict(env_file=".env")


#cache the settings results
@lru_cache
def get_settings()->Settings:
    return Settings()

