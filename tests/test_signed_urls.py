from datetime import UTC, timedelta, datetime

import pytest

from blobby import SignedUrlMethod
from storage_contexts import (
    StorageContext,
    azure_blob_storage,
    filesystem_storage,
    gcp_storage,
    memory_storage,
    s3_storage,
)


CLOUD_STORAGE_CONTEXTS = [
    s3_storage,
    # TODO: Implement Azure testing with Azurite
    # azure_blob_storage,
    # NOTE: Real credentials and real endpoints are needed to test GCP for now,
    # so we don't by default.
    # gcp_storage,
]


NON_CLOUD_STORAGE_CONTEXTS = [
    filesystem_storage,
    memory_storage,
]


@pytest.mark.parametrize("storage_context", CLOUD_STORAGE_CONTEXTS)
def test_generate_signed_url_get(storage_context: StorageContext) -> None:
    with storage_context() as storage:
        key = "test-files/sample.txt"
        data = b"hello world from signed url"

        storage.put(key=key, data=data)

        signed_url = storage.generate_signed_url(key, method=SignedUrlMethod.GET)

        assert isinstance(signed_url, str)
        assert len(signed_url) > 0
        assert "http" in signed_url.lower()
        assert "signature" in signed_url.lower()
        assert "expires" in signed_url.lower()
        # TODO: Update assertions for Azure URLs: Azure uses sig and se
        # assert ("signature" in signed_url.lower() or "sig=" in signed_url.lower())
        # assert ("expires" in signed_url.lower() or "se=" in signed_url.lower())


@pytest.mark.parametrize("storage_context", CLOUD_STORAGE_CONTEXTS)
def test_generate_signed_url_put(storage_context: StorageContext):
    with storage_context() as storage:
        key = "test-files/upload.txt"

        signed_url = storage.generate_signed_url(key, method=SignedUrlMethod.PUT)

        assert isinstance(signed_url, str)
        assert len(signed_url) > 0
        assert "http" in signed_url.lower()
        assert "signature" in signed_url.lower()
        assert "expires" in signed_url.lower()


@pytest.mark.parametrize("storage_context", CLOUD_STORAGE_CONTEXTS)
def test_generate_signed_url_default_expiration(storage_context: StorageContext):
    with storage_context() as storage:
        key = "test-files/default-expiry.txt"
        data = b"test default expiration"

        storage.put(key=key, data=data)

        # TODO: This is AWS-specific, which we can get away with when we're only testing S3
        one_hour = str(int((datetime.now(UTC) + timedelta(hours=1)).timestamp()))
        signed_url = storage.generate_signed_url(key)

        assert isinstance(signed_url, str)
        assert "expires" in signed_url.lower()
        assert one_hour in signed_url


@pytest.mark.parametrize("storage_context", CLOUD_STORAGE_CONTEXTS)
def test_generate_signed_url_custom_expiration(storage_context: StorageContext):
    with storage_context() as storage:
        key = "test-files/custom-expiry.txt"
        data = b"test custom expiration"

        storage.put(key=key, data=data)
        # TODO: This is AWS-specific, which we can get away with when we're only testing S3
        one_hour = str(int((datetime.now(UTC) + timedelta(hours=1)).timestamp()))
        signed_url = storage.generate_signed_url(key, expiration=timedelta(minutes=30))

        assert isinstance(signed_url, str)
        assert len(signed_url) > 0
        # TODO: Check for `se` URL parameter and Azure timestamp format if testing Azure
        assert "expires" in signed_url.lower()
        assert one_hour not in signed_url.lower()


@pytest.mark.parametrize("storage_context", CLOUD_STORAGE_CONTEXTS)
def test_generate_signed_url_different_methods(storage_context: StorageContext):
    with storage_context() as storage:
        key = "test-files/method-test.txt"

        get_url = storage.generate_signed_url(key, method=SignedUrlMethod.GET)
        put_url = storage.generate_signed_url(key, method=SignedUrlMethod.PUT)

        assert isinstance(get_url, str)
        assert isinstance(put_url, str)
        assert get_url != put_url


@pytest.mark.parametrize("storage_context", NON_CLOUD_STORAGE_CONTEXTS)
def test_generate_signed_url_not_supported(storage_context: StorageContext):
    with storage_context() as storage:
        key = "test-files/sample.txt"

        with pytest.raises(NotImplementedError):
            storage.generate_signed_url(key)
