from marshmallow import Schema, fields, validates, ValidationError
from config import Config
from typing import Optional


class DownloadRequestSchema(Schema):
    ft = fields.String(load_default=None, data_key="ft")

    @validates("ft")
    def _validate_ft(self, value: Optional[str]):
        if not value:
            return
        allowed = Config.ALLOWED_FILE_TYPES
        if value not in allowed:
            raise ValidationError(f"Invalid file type '{value}'. Allowed: {', '.join(sorted(allowed))}")
