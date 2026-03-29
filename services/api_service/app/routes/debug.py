import os
from fastapi import APIRouter

router = APIRouter(tags=["debug"])

@router.get("/debug/env")
async def debug_environment():
    # Only expose specific variables to verify mapping
    return {
        key: value for key, value in os.environ.items() 
        if key.startswith("PRIVATE_") or key.endswith("_URL") or key == "RENDER"
    }
