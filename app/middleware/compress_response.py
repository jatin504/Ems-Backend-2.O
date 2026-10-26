import gzip
from quart import Quart, Response, request, make_response

def compress_response(app: Quart):
    @app.after_request
    async def _compress_response(response: Response):
        # Skip if already encoded
        if response.headers.get("Content-Encoding"):
            return response

        # Only compress JSON and text
        content_type = response.headers.get("Content-Type", "")
        if not (
            content_type.startswith("application/json") or
            content_type.startswith("text/")
        ):
            return response

        # Check client support
        if "gzip" not in request.headers.get("Accept-Encoding", "").lower():
            return response

        # Get body
        body = await response.get_data()
        if isinstance(body, str):
            body = body.encode(response.charset or "utf-8")

        # Skip small responses
        if len(body) < 400:
            return response

        # Compress
        compressed_body = gzip.compress(body)

        # Make a new response
        new_response = await make_response(compressed_body, response.status_code)
        new_response.headers.update(response.headers)
        new_response.headers["Content-Encoding"] = "gzip"
        new_response.headers["Vary"] = "Accept-Encoding"
        new_response.headers["Content-Length"] = str(len(compressed_body))

        return new_response

    return _compress_response
