import os
import re
import io
import tarfile
from typing import Optional

from flask import send_file
from flask_smorest import Blueprint, abort

from routes.schemas.download import DownloadRequestSchema
from config import Config

blp = Blueprint(
    "Download",
    __name__,
    url_prefix="/download",
    description="Download electronic document files",
)


@blp.route("", methods=["GET"], strict_slashes=False)
@blp.arguments(DownloadRequestSchema, location="query", as_kwargs=True)
def download_file(**query_kwargs):
    # Extract parameters
    filename: Optional[str] = query_kwargs.get("filename")
    tar_path: Optional[str] = query_kwargs.get("tar_path")

    # Define the regex pattern to extract relative tar path
    tar_rel_regex = r"((?:stg|prd)-modula-\d{5}/\d{2}/\d{2}/\d{2}/\d{3}_\d{2}-\d{2}\.tar\.gz)$"
    
    # Extract relative tar path
    tar_rel_search = re.search(tar_rel_regex, tar_path)
    if not tar_rel_search:
        abort(400, message="Invalid 'tar_path' format - dnmep")
    
    tar_rel_path = tar_rel_search.group(1)
    if not tar_rel_path:
        abort(400, message="Invalid 'tar_path' format - cnerp")

    # Construct absolute tar path
    tar_abs_path = os.path.join(Config.FILES_ROOT, tar_rel_path)

    try:
        # Open the tar file and extract the requested file
        with tarfile.open(tar_abs_path, "r:gz") as tar:
            try:
                member = tar.getmember(filename)
                extracted_file = tar.extractfile(member)
                if extracted_file is None:
                    abort(404, message="Could not find the requested file")

                file_bytes = extracted_file.read()

            except KeyError:
                abort(404, message="Could not find the requested file")

        return send_file(
            io.BytesIO(file_bytes),
            as_attachment=True,
            download_name=filename,
        )
    except FileNotFoundError:
        abort(404, message="Could not find the requested tar archive")
    except tarfile.TarError:
        abort(500, message="Error processing the tar archive")
    except Exception as e:
        # Allow explicit aborts/HTTP exceptions to propagate without wrapping
        if hasattr(e, "status_code") or hasattr(e, "code"):
            raise
        abort(500, message=f"Unexpected error: {str(e)}")
