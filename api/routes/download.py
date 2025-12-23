from typing import List, Optional

from flask import send_file
from flask_smorest import Blueprint, abort

from config import Config
from routes.schemas.download import DownloadRequestSchema

blp = Blueprint(
    "Download",
    __name__,
    url_prefix="/download",
    description="Download electronic document files",
)


def _resolve_file_path(clave: str, file_type: str) -> Optional[str]:
    """
    Placeholder resolver that should map (clave, file_type) to a local file path.
    Replace this logic with the actual storage lookup in the mounted GCP directory.
    """
    # TODO: Implement real lookup based on storage layout
    return None


@blp.route("/<string:clave>", methods=["GET"], strict_slashes=False)
@blp.arguments(DownloadRequestSchema, location="query", as_kwargs=True)
def download_file(clave: str, **query_kwargs):
    file_types: List[str] = query_kwargs.get("file_types") or []

    # default to all allowed types if none specified
    search_types = file_types or list(Config.ALLOWED_FILE_TYPES)

    for ft in search_types:
        path = _resolve_file_path(clave, ft, Config.FILES_ROOT)
        if path:
            try:
                return send_file(path, as_attachment=True)
            except Exception:
                abort(500, message="Could not send the file")

    abort(404, message="Could not find the requested file")
