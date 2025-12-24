from marshmallow import Schema, fields, validate

class MoveToBucketRequestSchema(Schema):
    customer_id = fields.String(required=True, validate=validate.Regex(r"^(stg|prd)-modula-\d{5}$"))