import os
import glob
import shutil
from os import path
from pathlib import Path

from filelock import FileLock
from tqdm import tqdm

from storage.azure_storage import get_corpus_container_service_client
from storage.processed_manifest import load_processed_manifest

blob_storage_results_folder = "output"
local_results_dir_uploaded_suffix = "uploaded"


def list_mets_files(root_dir):
    return sorted(list(glob.glob(path.join(root_dir, "**/*-METS.xml"), recursive=True)))

def blob_required_for_processing(blob_name: str):
    return not blob_name.endswith("pdf") # we dont need the pdf files

def download_entry(corpus_entry: str, data_dir: str):
    container_client = get_corpus_container_service_client()
    # download the entire folder of this entry.
    # Remove the "file" compoent
    entry_blob_folder = Path(corpus_entry).parent
    local_entry_blob_folder = path.join(data_dir, entry_blob_folder)

    # if exists abort
    if os.path.exists(local_entry_blob_folder):
        return

    all_blobs_on_path = container_client.list_blobs(
        name_starts_with=str(entry_blob_folder)
    )

    for blob in all_blobs_on_path:
        # don't download blobs that are not required for the actual processing
        if not blob_required_for_processing(blob.name):
            continue
        
        blob_client = container_client.get_blob_client(blob.name)
        local_file_path = path.join(data_dir, blob.name)
        local_dir_path = path.dirname(local_file_path)
        # Create the directory if it doesn't exist
        Path(local_dir_path).mkdir(parents=True, exist_ok=True)

        # Download the blob to a local file
        with open(local_file_path, "wb") as blob_file:
            blob_file.write(blob_client.download_blob().readall())


def remove_local_entry(corpus_entry: str, data_dir: str):
    entry_blob_folder = Path(corpus_entry).parent
    local_entry_blob_folder = path.join(data_dir, entry_blob_folder)

    # if exists abort remove all dir
    if os.path.exists(local_entry_blob_folder):
        shutil.rmtree(local_entry_blob_folder)


def download_corpus_subset(
    corpus_entries_to_download: list[str], data_dir: str, processed_manifest_file: str
) -> None:
    # get all entries locally stored
    local_entries = list_mets_files(data_dir)

    # strip of the root data dir to get corpus entries
    local_entries = [entry.replace(f"{data_dir}/", "") for entry in local_entries]

    # required entries are entries not locally stored but requested in the download request
    required_to_download = set(corpus_entries_to_download) - set(local_entries)

    for to_download_entry in tqdm(required_to_download, desc="Downloading corpus data"):
        download_entry(to_download_entry, data_dir)

    # Cleanup from local storage any processed entries
    processed_entries = load_processed_manifest(processed_manifest_file)

    # not needed locally are processsed entries
    no_longer_locally_needed = set(local_entries).intersection(processed_entries)

    for entry_to_cleanup in tqdm(
        no_longer_locally_needed, desc="Cleanup processed local corpus data"
    ):
        remove_local_entry(entry_to_cleanup, data_dir)


def upload_corpus_results(output_dir: str, results_csv_files: list[str] = None):
    uploaded_output_dir = f"{output_dir}_uploaded"
    container_client = get_corpus_container_service_client()
    lock = FileLock("upload.results.lock")
    with lock:
        # list all csv results files in results dir
        if results_csv_files is None:
            # upload anything if not specified which files to upload
            results_csv_files = glob.glob(f"{output_dir}/*.csv")

        for result_file in tqdm(
            results_csv_files, desc="uploading results to blob storage"
        ):
            blob_result_file = result_file.replace(
                output_dir, blob_storage_results_folder
            )
            container_client.upload_blob(
                blob_result_file, data=open(result_file, "rb"), overwrite=True
            )

            # move to the uploaded folder (so it won't be reprocessed)
            move_to_file = result_file.replace(output_dir, uploaded_output_dir)
            os.makedirs(Path(move_to_file).parent, exist_ok=True)
            os.replace(result_file, move_to_file)
