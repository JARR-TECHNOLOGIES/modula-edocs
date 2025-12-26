from flask import g, jsonify


def add_response_wrapper_middleware(app):
    @app.after_request
    def wrap_response(response):
        # Only wrap JSON responses
        if response.is_json:
            body = response.get_json(silent=True)

            # Do not wrap error responses; let error handlers return their own shape.
            if response.status_code >= 400:
                return response

            if isinstance(body, dict) and "ok" not in body:
                wrapped = {
                    "ok": True,
                    "code": "SUCCESS",
                    "message": "",
                    "data": body,
                    "request_id": getattr(g, "request_id", None),
                }
                new_response = jsonify(wrapped)
                new_response.status_code = response.status_code
                # preserve headers already set
                for key, value in response.headers.items():
                    if key.lower() != "content-length":
                        new_response.headers[key] = value
                return new_response

        return response
