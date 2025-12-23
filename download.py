from typing import List, Optional

from flask import send_file
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields, validates, ValidationError, post_load

from config import Config

blp = Blueprint(
    "Download",
    __name__,
    url_prefix="/download",
    description="Download electronic document files",
)


class DownloadRequestSchema(Schema):
    ik = fields.String(required=True, data_key="ik")
    ft = fields.String(load_default=None, data_key="ft")

    @validates("ft")
    def _validate_ft(self, value: Optional[str]):
        if not value:
            return
        allowed = Config.ALLOWED_FILE_TYPES
        for item in value.split(","):
            if item and item not in allowed:
                raise ValidationError(f"Invalid file type '{item}'. Allowed: {', '.join(sorted(allowed))}")

    @post_load
    def _split_ft(self, data, **kwargs):
        ft_raw = data.get("ft")
        if ft_raw:
            data["file_types"] = [item for item in ft_raw.split(",") if item]
        else:
            data["file_types"] = []
        return data


def _resolve_file_path(clave: str, idempotency_key: str, file_type: str, files_root: str) -> Optional[str]:
    """
    Placeholder resolver that should map (clave, idempotency_key, file_type) to a local file path.
    Replace this logic with the actual storage lookup in the mounted GCP directory.
    """
    # TODO: Implement real lookup based on storage layout
    return None


@blp.route("/<string:clave>", methods=["GET"], strict_slashes=False)
@blp.arguments(DownloadRequestSchema, location="query", as_kwargs=True)
def download_file(clave: str, **query_kwargs):
    idempotency_key = query_kwargs.get("ik")
    file_types: List[str] = query_kwargs.get("file_types") or []

    # default to all allowed types if none specified
    search_types = file_types or list(Config.ALLOWED_FILE_TYPES)

    for ft in search_types:
        path = _resolve_file_path(clave, idempotency_key, ft, Config.FILES_ROOT)
        if path:
            try:
                return send_file(path, as_attachment=True)
            except Exception:
                abort(500, message="No se pudo descargar el archivo")

    abort(404, message="Archivo no encontrado")
