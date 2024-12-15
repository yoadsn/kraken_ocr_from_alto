import os

from azure.storage.blob import ContainerClient


def get_corpus_container_service_client() -> ContainerClient:
    return ContainerClient(
        account_url=os.getenv("AZURE_STORAGE_SAS_URL"),
        container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
    )
