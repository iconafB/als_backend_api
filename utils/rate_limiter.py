from slowapi import Limiter,_rate_limit_exceeded_handler
from fastapi import FastAPI
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


USER_READ_LIMIT="200/minute"
USER_WRITE_LIMIT="50/minute"
USER_DELETE_LIMIT="10/minute"
#Per-client key (by IP)
rate_limiter=Limiter(key_func=get_remote_address)

def setup_rate_limiter(app:FastAPI):
    #app is an app instance from fastapi dummy
    #This function attaches the limiter + exception handler to that same app instance
    app.state.limiter=rate_limiter

    app.add_exception_handler(RateLimitExceeded,_rate_limit_exceeded_handler)
