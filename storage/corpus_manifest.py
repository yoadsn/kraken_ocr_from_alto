import os

from storage.azure_storage import get_corpus_container_service_client

mets_file_suffix = "METS.xml"
default_manifest_file = "corpus.manifest.txt"
exclude_root_dir_name = "Forverts"


def load_manifest(manifest_file: str = default_manifest_file) -> list[str]:
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
        raise FileNotFoundError(
            f"Manifest file {manifest_file} not found - need to generate?"
        )


def upload_manifest_to_blob_storage(manifest_file: str):
    container_service = get_corpus_container_service_client()
    # Upload the manifest file to Azure Blob Storage root
    with open(manifest_file, "rb") as data:
        container_service.upload_blob("corpus.manifest.txt", data, overwrite=True)


def generate_corpus_manifest(output_file: str):
    container_service = get_corpus_container_service_client()
    with open(output_file, "w") as f:
        # List the blobs in the container
        blob_list = container_service.list_blobs()
        for blob in blob_list:
            if blob.name.endswith(mets_file_suffix) and not blob.name.startswith(
                exclude_root_dir_name
            ):
                # Write the blob name to the manifest file
                f.write(f"{blob.name}\n")


def report_manifest_stats(manifest_file: str):
    manifest = load_manifest(manifest_file)
    print(f"Number of items in manifest: {len(manifest)}")


def get_total_in_corpus(manifest_file: str):
    manifest = load_manifest(manifest_file)
    return len(manifest)
