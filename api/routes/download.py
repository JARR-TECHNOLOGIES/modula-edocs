from typing import Optional

from flask import send_file
from flask_smorest import Blueprint, abort

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
    # Validate clave length
    if len(clave) != 50:
        abort(400, message="Invalid 'clave'")

    # Get requested file type
    requested_type: Optional[str] = query_kwargs.get("ft")

    # Try to resolve the file path
    path = _resolve_file_path(clave, requested_type)
    if path:
        try:
            return send_file(path, as_attachment=True)
        except Exception:
            abort(500, message="Could not send the file")

    abort(404, message="Could not find the requested file")