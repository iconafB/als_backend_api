from pathlib import Path
from fastapi import HTTPException, status

# This resolves relative to *this* file's location:
# .../als_backend_api/utils/data_insertion/file_name_resolver.py
# We want project root (folder containing main.py), so go up 2 levels:
# data_insertion -> utils -> als_backend_api
BASE_DIR = Path(__file__).resolve().parents[2]  # .../als_backend_api


def resolve_file_path(filename: str) -> Path:
    """
    Resolve a filename relative to the project directory (folder containing main.py).
    Never returns None: returns a Path or raises HTTPException.
    """

    if filename is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="filename is required",
        )

    filename = filename.strip().strip('"').strip("'")
    if not filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="filename cannot be empty",
        )

    file_path = (BASE_DIR / filename)

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "message": f"File not found: {filename}",
                "base_dir": str(BASE_DIR),
                "tried_path": str(file_path),
            },
        )

    return file_path



