from marshmallow import Schema, fields, validates, ValidationError, post_load
from config import Config
from typing import Optional

class DownloadRequestSchema(Schema):
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
