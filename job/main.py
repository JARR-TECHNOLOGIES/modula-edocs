import os
import re
import sys
import base64
import tarfile
import shutil
import argparse
from pathlib import Path
from datetime import timedelta

from job.logging import get_logger, setup_logging
from job.db import get_client
from job.time import from_local_to_utc, get_timestamp

logger = None

# CLI argument parsing
def parse_args():
    parser = argparse.ArgumentParser(description="Move staged files to bucket")
    parser.add_argument(
        "--customer-id",
        required=True,
        help="Customer identifier (tenant database name)",
    )
    return parser.parse_args()

# Streaming-safe base64 decoding and file writing
BASE64_CHUNK_SIZE = 8192  # 8 KB
_B64_ALPHABET = set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
_B64_WHITESPACE = set(b" \n\r\t")


def _is_probably_base64(data: bytes) -> bool:
    """
    Heuristic check to determine if data is probably base64-encoded.
    
    Args:
        data (bytes): Data to check.
        
    Returns:
        bool: True if data is probably base64-encoded, False otherwise.
    """
    sample = data.strip()
    if not sample:
        return False

    remainder = len(sample) % 4
    if remainder == 1:
        return False

    # Check characters in sample
    for ch in sample[:4096]:
        if ch in _B64_WHITESPACE:
            continue
        if ch not in _B64_ALPHABET:
            return False
    return True


def write_base64_stream(b64_data: str | bytes, out_path: Path) -> None:
    """
    Write base64-encoded data to a file in a streaming-safe manner.

    Args:
        b64_data (str | bytes): Base64-encoded data.
        out_path (Path): Output file path.
    """
    if isinstance(b64_data, (bytes, bytearray, memoryview)):
        b64_bytes = bytes(b64_data)
    elif isinstance(b64_data, str):
        b64_bytes = b64_data.encode("utf-8")
    else:
        b64_bytes = str(b64_data).encode("utf-8")

    # If payload is raw binary (BSON Binary), write as-is in chunks
    if not _is_probably_base64(b64_bytes):
        with open(out_path, "wb") as f:
            for i in range(0, len(b64_bytes), BASE64_CHUNK_SIZE):
                f.write(b64_bytes[i:i + BASE64_CHUNK_SIZE])
        return

    with open(out_path, "wb") as f:
        buffer = b""

        for i in range(0, len(b64_bytes), BASE64_CHUNK_SIZE):
            buffer += b64_bytes[i:i + BASE64_CHUNK_SIZE]

            # Only decode full base64 quanta
            excess = len(buffer) % 4
            if excess:
                to_decode = buffer[:-excess]
                buffer = buffer[-excess:]
            else:
                to_decode = buffer
                buffer = b""

            if to_decode:
                f.write(base64.b64decode(to_decode))

        if buffer:
            padding = (-len(buffer)) % 4
            if padding:
                buffer += b"=" * padding
            f.write(base64.b64decode(buffer))


def clave_to_rel_path(clave: str) -> Path:
    """Return the relative staging path components derived from clave."""
    year = clave[7:9]
    day = clave[6:8]
    month = clave[5:7]
    branch_code = clave[21:24]
    edoc_type = clave[29:31]

    return Path(year) / month / day / branch_code / edoc_type


# Streaming-safe staging of files to disk
def stage_file_streaming(
    *,
    customer_id: str,
    clave: str,
    filename: str,
    binary_b64: str,
) -> None:
    """
    Stage a single file to disk in a structured folder layout based on the clave.

    Args:
        customer_id (str): Customer identifier.
        clave (str): Electronic document clave.
        filename (str): Original filename.
        binary_b64 (str): Base64-encoded binary content.

    Returns:
        None
    """
    stage_root = Path(os.getenv("STAGE_FILES_ROOT", "/tmp/staged-files"))

    if len(clave) < 50:
        raise ValueError("Invalid clave length")

    stage_folder = stage_root / customer_id / clave_to_rel_path(clave)

    stage_folder.mkdir(parents=True, exist_ok=True)

    safe_filename = Path(filename).name
    file_path = stage_folder / safe_filename

    logger.debug(f"Staging file {file_path}")
    write_base64_stream(binary_b64, file_path)


def update_documents_for_tarball(
    *,
    db,
    coll_name: str,
    oids: set,
    tarball_path: Path,
    moved_at,
) -> None:
    """Update documents for files that were added to a tarball."""
    if not oids:
        return

    tar_str = str(tarball_path)
    coll = db.get_collection(coll_name)
    logger.info(f"Updating {len(oids)} documents in collection {coll_name} with tarball {tar_str}")

    def merge_file(path: str):
        file_exists = {"$ne": [{"$type": f"${path}"}, "missing"]}
        file_doc = {"$ifNull": [f"${path}", {}]}

        return {
            "$cond": [
                file_exists,
                {
                    "$mergeObjects": [
                        file_doc,
                        {
                            "status": "bucket",
                            "moved_at": moved_at,
                            "binary": None,
                            "tarball_path": tar_str,
                        },
                    ]
                },
                "$$REMOVE",
            ]
        }

    update_pipeline = [
        {
            "$set": {
                "files.pdf": merge_file("files.pdf"),
                "files.html": merge_file("files.html"),
                "files.xml": merge_file("files.xml"),
                "files.mh_xml": merge_file("files.mh_xml"),
                "files.mr_xml": merge_file("files.mr_xml"),
            }
        }
    ]

    res = coll.update_many({"_id": {"$in": list(oids)}}, update_pipeline)
    logger.info(f"Updated {res.modified_count} documents in collection {coll_name}")
    logger.debug(f"Update result: {res.raw_result}")


# Streaming-safe saving of staged files to tar.gz in bucket
def save_staged_files(
    customer_id: str,
    db,
    staged_docs_by_dir: dict[str, dict[str, set]],
    *,
    now_local,
) -> None:
    """
    Save all staged files for a customer into tar.gz archives in the bucket.

    Args:
        customer_id (str): Customer identifier.
    
    Returns:
        None
    """
    stage_root = Path(os.getenv("STAGE_FILES_ROOT", "/tmp/staged-files")) / customer_id
    if not stage_root.exists():
        logger.info("No staged files found")
        return

    hhmm = now_local.strftime("%H-%M")

    edoc_dirs = [
        p for p in stage_root.rglob("*")
        if p.is_dir() and len(p.relative_to(stage_root).parts) == 5
    ]

    logger.info(f"Found {len(edoc_dirs)} edoc directories to process")

    for edoc_dir in edoc_dirs:
        logger.debug(f"Processing edoc directory {edoc_dir}")

        # Determine tarball path
        rel_path = edoc_dir.relative_to(stage_root)
        *parent_parts, edoc_type = rel_path.parts
        rel_key = "/".join(rel_path.parts)

        bucket_dir = (
            Path(os.getenv("FILES_ROOT", "/gcp-bucket"))
            / customer_id
            / Path(*parent_parts)
        )
        bucket_dir.mkdir(parents=True, exist_ok=True)

        tar_tmp_root = Path(os.getenv("TAR_TMP_ROOT", "/tmp/tar-tmp"))
        tar_tmp_root.mkdir(parents=True, exist_ok=True)

        tar_name = f"{edoc_type}_{hhmm}.tar.gz"
        tar_tmp_path = tar_tmp_root / tar_name
        tar_path = bucket_dir / tar_name

        logger.info(f"Creating tarball {tar_tmp_path} then moving to {tar_path}")

        # Create tar.gz archive outside the bucket mount, then move it in
        with tarfile.open(str(tar_tmp_path), mode="w|gz") as tar:
            for file_path in edoc_dir.iterdir():
                if file_path.is_file():
                    logger.debug(f"Adding file to tarball: {file_path}")
                    tar.add(file_path, arcname=file_path.name)

        # Move tarball into bucket mount (handles cross-device moves)
        shutil.move(str(tar_tmp_path), str(tar_path))

        # Clean up staged files
        for file_path in edoc_dir.iterdir():
            if file_path.is_file():
                logger.debug(f"Removing staged file: {file_path}")
                file_path.unlink()

        edoc_dir.rmdir()

        # Update MongoDB documents associated with this tarball
        coll_map = staged_docs_by_dir.get(rel_key, {})
        for coll_name, oid_set in coll_map.items():
            update_documents_for_tarball(
                db=db,
                coll_name=coll_name,
                oids=oid_set,
                tarball_path=tar_path,
                moved_at=now_local,
            )


#  Job main logic
def run_job(customer_id: str) -> None:
    """
    Main job logic to move staged files to bucket.

    Args:
        customer_id (str): Customer identifier.
    """
    logger.info("Cloud Run job started")
    client = get_client()
    db = client[customer_id]

    staged_docs_by_dir: dict[str, dict[str, set]] = {}

    now_local = get_timestamp()
    now_utc = from_local_to_utc(now_local)
    three_hours_ago_utc = now_utc - timedelta(hours=3)

    collections = [
        "edocs_facturas_electronicas",
        "edocs_notas_credito_electronicas",
        "edocs_tiquetes_electronicos",
        "edocs_ordenes_internas",
        "edocs_proformas",
    ]

    file_fields = [
        "files.pdf",
        "files.html",
        "files.xml",
        "files.mh_xml",
        "files.mr_xml",
    ]

    status_filters = []
    for base_field in file_fields:
        exists_filter = {base_field: {"$exists": True}}
        status_field = f"{base_field}.status"

        # Only apply status checks if the file entry exists
        status_filters.append({"$and": [exists_filter, {status_field: "staged"}]})
        status_filters.append({"$and": [exists_filter, {status_field: {"$exists": False}}]})

    query = {
        "edoc_json.FechaEmision": {"$lte": three_hours_ago_utc},
        "$or": status_filters,
    }
    
    for coll_name in collections:
        logger.info(f"Processing collection: {coll_name}")
        
        coll = db.get_collection(coll_name)
        
        cursor = list(coll.find(query, {"edoc_json.Clave": 1, "files": 1}))
        logger.info(f"Found {len(cursor)} documents in collection {coll_name}")

        for doc in cursor:
            clave = doc.get("edoc_json", {}).get("Clave", "unknown")
            logger.debug(f"Processing document clave={clave}")
            files = doc.get("files", {})

            for file_data in files.values():
                if not file_data.get("binary"):
                    logger.debug(f"No binary data for file {file_data.get('filename')}, skipping")
                    continue

                if file_data.get("status", "staged") != "staged":
                    logger.debug(f"Skipping file {file_data.get('filename')} with status {file_data.get('status')}")
                    continue

                logger.debug(f"Staging file {file_data.get('filename')} for clave={clave}")

                stage_file_streaming(
                    customer_id=customer_id,
                    clave=clave,
                    filename=file_data["filename"],
                    binary_b64=file_data["binary"],
                )

                rel_dir = clave_to_rel_path(clave)
                rel_key = "/".join(rel_dir.parts)
                staged_docs_by_dir.setdefault(rel_key, {}).setdefault(coll_name, set()).add(doc["_id"])

            logger.debug(f"Completed processing document clave={clave}")

    if not staged_docs_by_dir:
        logger.info("No staged files found to move")
        return
    
    logger.info(f"Staged files found in {len(staged_docs_by_dir)} edoc directories, proceeding to save")

    save_staged_files(customer_id, db, staged_docs_by_dir, now_local=now_local)

    logger.info("Cloud Run job completed successfully")

# Entry point
if __name__ == "__main__":
    try:
        # Parse arguments
        args = parse_args()

        # Get customer ID
        customer_id = args.customer_id

        # Setup logging
        setup_logging()
        logger = get_logger(__name__, module_name="UploadJob", customer_id=customer_id)

        # Validate customer ID format
        if customer_id is None or not re.match(r"^(stg|prd)-modula-\d{5}$", customer_id):
            logger.error(f"Invalid customer ID format: {customer_id}")
            sys.exit(1)
        
        # Check required environment variables
        required_env_vars = ["MONGO_USERNAME", "MONGO_PASSWORD", "MONGO_CLUSTER"]

        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            sys.exit(1)

        # Run the job
        run_job(customer_id=customer_id)
        sys.exit(0)
    except Exception as exc:
        logger.exception(f"Job failed: {exc}")
        sys.exit(1)
