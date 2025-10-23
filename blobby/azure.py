from datetime import datetime, timedelta, timezone

try:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import (
        BlobSasPermissions,
        ContainerClient,
        generate_blob_sas,
    )
except ImportError as e:
    raise ImportError(
        "Azure support is not installed, run `pip install blobby[azure]`"
    ) from e

from blobby import Storage
from blobby.storage import ObjectMeta, SignedUrlMethod


class AzureBlobStorage(Storage):
    def __init__(self, client: ContainerClient):
        self._client = client

    def _put(self, key: str, data: bytes) -> None:
        self._client.upload_blob(key, data)

    def get(self, key: str) -> bytes:
        try:
            return self._client.download_blob(key).read()
        except ResourceNotFoundError:
            self.raise_key_not_found(key)

    def delete(self, key: str) -> None:
        try:
            self._client.delete_blob(key)
        except ResourceNotFoundError:
            self.raise_key_not_found(key)

    def list(self, prefix: str) -> list[ObjectMeta]:
        blobs = self._client.list_blobs(name_starts_with=prefix)

        return [ObjectMeta(key=b.name) for b in blobs]

    def generate_signed_url(
        self,
        key: str,
        *,
        expiration: timedelta = timedelta(hours=1),
        method: SignedUrlMethod = SignedUrlMethod.GET,
    ) -> str:
        permissions = (
            BlobSasPermissions(read=True)
            if method == SignedUrlMethod.GET
            else BlobSasPermissions(write=True, create=True)
        )

        expiry_time = datetime.now(timezone.utc) + expiration

        sas_token = generate_blob_sas(
            account_name=self._client.account_name,
            container_name=self._client.container_name,
            blob_name=key,
            account_key=self._client.credential.account_key,
            permission=permissions,
            expiry=expiry_time,
        )

        blob_client = self._client.get_blob_client(key)
        return f"{blob_client.url}?{sas_token}"
