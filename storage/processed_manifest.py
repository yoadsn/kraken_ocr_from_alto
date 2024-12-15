import os

from filelock import FileLock

from storage.azure_storage import get_corpus_container_service_client

default_processed_manifest_file = "processed.manifest.txt"


def load_processed_manifest(
    manifest_file: str = default_processed_manifest_file,
) -> list[str]:
    container_service = get_corpus_container_service_client()
    # It the manifest cannot be found locally
    if not os.path.isfile(manifest_file):
        # Try and download it from the corpus root
        blob_client = container_service.get_blob_client(manifest_file)
        if blob_client.exists():
            with open(manifest_file, "wb") as mf:
                mf.write(blob_client.download_blob().readall())

    # If this works read the manifest
    if os.path.isfile(manifest_file):
        with open(manifest_file, "r") as f:
            return [line.strip() for line in f.readlines()]

    else:
        # Create an empty processed file locally and upload it
        with open(manifest_file, "w") as mf:
            mf.write("")

        # Upload the manifest
        container_service.upload_blob(manifest_file, manifest_file)

        return []


def upload_processed_manifest(
    manifest_file: str = default_processed_manifest_file,
) -> None:
    container_service = get_corpus_container_service_client()
    # Upload the manifest
    with open(manifest_file, "rb") as f:
        container_service.upload_blob(manifest_file, f, overwrite=True)


def append_to_processed_manifest(
    processed: list[str], manifest_file: str = default_processed_manifest_file
):
    lock = FileLock("processed.manifest.txt.lock")
    with lock:
        with open(manifest_file, "a") as f:
            for processed_file in processed:
                f.write(processed_file + "\n")

        # Upload the manifest
        upload_processed_manifest(manifest_file)


def cleanup_processed_manifest(
    manifest_file: str = default_processed_manifest_file,
) -> list[str]:
    container_service = get_corpus_container_service_client()
    if os.path.isfile(manifest_file):
        os.remove(manifest_file)

    # also delete from the blob storage
    blob_client = container_service.get_blob_client(manifest_file)
    blob_client.delete_blob()


def get_processed_count(manifest_file: str):
    manifest = load_processed_manifest(manifest_file)
    return len(manifest)
