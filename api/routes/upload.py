import base64
import tarfile

from flask_smorest import Blueprint, abort
from pathlib import Path
from extensions.logging import get_logger
from extensions.db import get_client
from api.utils.time import from_local_to_utc, get_timestamp
from datetime import timedelta
from routes.schemas.upload import MoveToBucketRequestSchema
from config import Config

blp = Blueprint(
    "Upload",
    __name__,
    url_prefix="/upload",
    description="Upload electronic document files",
)

logger = get_logger(__name__, module_name="Upload")


def stage_file(customer_id:str, clave: str, filename: str, binary_b64: str) -> bool:
    """
    """
    logger.info(f"Staging file with clave={clave}, filename={filename}, size={len(binary_b64)} bytes")

    stage_root = Path(Config.STAGE_FILES_ROOT)

    year = clave[7:9]
    day = clave[6:8]
    month = clave[5:7]
    branch_code = clave[21:24]
    terminal_code = clave[24:29]
    edoc_type = clave[29:31]
    consecutive = clave[31:41]

    stage_folder = Path(stage_root) / customer_id / year / month / day / branch_code / terminal_code / edoc_type
    logger.debug(f"Parsed clave into year={year}, month={month}, day={day}, customer_id={customer_id}, branch_code={branch_code}, terminal_code={terminal_code}, edoc_type={edoc_type}, consecutive={consecutive}")
    
    try:
        # Base64 decode
        
        binary_data = base64.b64decode(binary_b64)
        # Ensure directory exists
        stage_folder.mkdir(parents=True, exist_ok=True)
        file_path = stage_folder / filename

        # Write the file
        with open(file_path, "wb") as f:
            f.write(binary_data)
        logger.info(f"Staged file at {file_path}")
        return True, ""
    except Exception as e:
        logger.error(f"Error staging file: {e}")
        return False, str(e)

def save_staged_files(customer_id: str):
    stage_root = Path(Config.STAGE_FILES_ROOT) / customer_id
    if not stage_root.exists():
        return False, "No staged files found for the customer"

    logger.info(f"Saving staged files for customer_id={customer_id} from {stage_root}")

    edoc_dirs = [
        p for p in stage_root.rglob("*")
        if p.is_dir() and len(p.relative_to(stage_root).parts) == 6
    ]

    if not edoc_dirs:
        return False, "No staged document directories found"

    for edoc_dir in edoc_dirs:
        rel_path = edoc_dir.relative_to(stage_root)
        *parent_parts, edoc_type = rel_path.parts

        bucket_dir = (
            Path(Config.FILES_ROOT)
            / customer_id
            / Path(*parent_parts)
        )
        bucket_dir.mkdir(parents=True, exist_ok=True)

        tar_path = bucket_dir / f"{edoc_type}.tar.gz"

        logger.info(f"Creating tarball {tar_path} from {edoc_dir}")

        with tarfile.open(tar_path, mode="w:gz") as tar:
            for file_path in edoc_dir.iterdir():
                if file_path.is_file():
                    tar.add(file_path, arcname=file_path.name)
                    logger.debug(f"Added {file_path.name} to {tar_path.name}")

    return True, ""


@blp.route("/run", methods=["POST"], strict_slashes=False)
@blp.arguments(MoveToBucketRequestSchema)
def move_to_bucket(payload: dict):
    customer_id = payload.get("customer_id")

    logger.info(f"Received move to bucket request for customer_id={customer_id}")

    # Get DB client
    client = get_client()

    # Select DB
    db = client[customer_id]

    staged_oids = {}

    now_local = get_timestamp()
    now_utc = from_local_to_utc(now_local)
    three_hours_ago_utc = now_utc - timedelta(hours=3)

    # Define collections
    collections_str = [
        "edocs_facturas_electronicas",
        "edocs_notas_credito_electronicas",
        "edocs_tiquetes_electronicos",
        "edocs_ordenes_internas",
        "edocs_proformas",
    ]
    
    # Define query to find documents with staged files and older than or equal to 3 hours
    query = {
        "edoc_json.FechaEmision": {
            "$lte": three_hours_ago_utc
        },
        "$or": [
            {"files.pdf.status": "staged"},
            {"files.html.status": "staged"},
            {"files.xml.status": "staged"},
            {"files.mh_xml.status": "staged"},
            {"files.mr_xml.status": "staged"},
        ]
    }
    
    # Access collections
    for coll_str in collections_str:
        coll = db.get_collection(coll_str)

        # Find documents with staged files
        pending_docs = coll.find(query, {"clave": 1, "files": 1})

        for doc in pending_docs:
            clave = doc.get("clave")
            files = doc.get("files", {})

            # Process each file type
            for _, file_data in files.items():
                if file_data.get("status") == "staged":
                    filename = file_data.get("filename")
                    binary_b64 = file_data.get("binary")

                    success, error_msg = stage_file(
                        customer_id=customer_id,
                        clave=clave,
                        filename=filename,
                        binary_b64=binary_b64,
                    )

                    if success:
                        if coll_str not in staged_oids:
                            staged_oids[coll_str] = set()
                            staged_oids[coll_str].add(doc["_id"])
                        else:
                            staged_oids[coll_str].add(doc["_id"])
                    else:
                        logger.error(f"Failed to stage file {filename} for clave={clave}: {error_msg}")

    # If no staged files found, return early
    if not staged_oids:
        logger.info(f"No staged files found older than 3 hours for customer_id={customer_id}.")
        return {
            "ok": True,
            "message": "No staged files to move",
            "code": "200",
            "data": {},
        }, 200
    
    # Move staged files to bucket
    logger.info(f"Moving staged files for customer_id={customer_id} to bucket...")
    
    success, error_msg = save_staged_files(customer_id=customer_id)

    if not success:
        logger.error(f"Error moving staged files to bucket for customer_id={customer_id}: {error_msg}")
        abort(500, message=f"Error moving staged files to bucket: {error_msg}")

    logger.info(f"Successfully moved staged files to bucket for customer_id={customer_id}.")

    logger.info(f"Updating document statuses in DB for customer_id={customer_id}...")
    # Update document statuses in DB
    for coll_str, oids in staged_oids.items():
        coll = db.get_collection(coll_str)
        result = coll.update_many(
            {
                "_id": {
                    "$in": list(oids)
                },
            },
            {
                "$set": {
                    "files.$[file].status": "saved_to_bucket",
                    "files.$[file].moved_at": now_local,
                    "files.$[file].binary": None
                }
            },
            array_filters=[{"file.status": "staged"}],
        )
        logger.info(f"Updated {result.modified_count} documents in collection {coll_str} for customer_id={customer_id}.")

    return {
        "ok": True,
        "message": "Move to bucket completed successfully",
        "code": "201",
        "data": {},
    }, 201