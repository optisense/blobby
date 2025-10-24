from datetime import timedelta

from blobby import Storage
from blobby.storage import ObjectMeta, SignedUrlMethod


class MemoryStorage(Storage):
    def __init__(self) -> None:
        self._storage: dict[str, bytes] = {}

    def _put(self, key: str, data: bytes) -> None:
        self._storage[key] = data

    def get(self, key: str) -> bytes:
        try:
            return self._storage[key]
        except KeyError:
            self.raise_key_not_found(key)

    def delete(self, key: str) -> None:
        try:
            del self._storage[key]
        except KeyError:
            self.raise_key_not_found(key)

    def list(self, prefix: str) -> list[ObjectMeta]:
        return [ObjectMeta(key=k) for k in self._storage.keys() if k.startswith(prefix)]

    def generate_signed_url(
        self,
        key: str,
        *,
        expiration: timedelta = timedelta(hours=1),
        method: SignedUrlMethod = SignedUrlMethod.GET,
    ) -> str:
        raise NotImplementedError("MemoryStorage does not support signed URLs")
