import hashlib
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from src.cpi_w1_artifacts import ArtifactIntegrityError, FilesystemArtifactStore


class CpiW1ArtifactStoreTest(unittest.TestCase):
    def test_each_artifact_uuid_gets_its_own_object_path(self) -> None:
        body = b"same bytes"
        sha = hashlib.sha256(body).hexdigest()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FilesystemArtifactStore(temp_dir)
            first = store.put(uuid4(), body, sha)
            second = store.put(uuid4(), body, sha)
            self.assertNotEqual(first.storage_uri, second.storage_uri)

    def test_same_artifact_and_bytes_is_idempotent(self) -> None:
        body = b"immutable"
        sha = hashlib.sha256(body).hexdigest()
        artifact_id = uuid4()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FilesystemArtifactStore(temp_dir)
            first = store.put(artifact_id, body, sha)
            second = store.put(artifact_id, body, sha)
            self.assertEqual(first.storage_uri, second.storage_uri)
            self.assertEqual(first.sha256, second.sha256)

    def test_storage_generation_is_stable_for_idempotent_reuse(self) -> None:
        body = b"immutable"
        sha = hashlib.sha256(body).hexdigest()
        artifact_id = uuid4()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FilesystemArtifactStore(temp_dir)
            first = store.put(artifact_id, body, sha)
            second = store.put(artifact_id, body, sha)
            self.assertEqual(first.storage_generation, second.storage_generation)

    def test_wrong_declared_hash_is_rejected_without_object(self) -> None:
        body = b"immutable"
        wrong = hashlib.sha256(b"other").hexdigest()
        artifact_id = uuid4()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FilesystemArtifactStore(temp_dir)
            with self.assertRaises(ArtifactIntegrityError):
                store.put(artifact_id, body, wrong)
            artifact_dir = Path(temp_dir) / str(artifact_id)
            self.assertFalse(artifact_dir.exists())

    def test_delete_uncommitted_requires_exact_generation_and_hash(self) -> None:
        body = b"orphan"
        sha = hashlib.sha256(body).hexdigest()
        artifact_id = uuid4()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FilesystemArtifactStore(temp_dir)
            stored = store.put(artifact_id, body, sha)
            path = Path(stored.storage_uri.removeprefix("file://"))
            self.assertTrue(path.exists())
            store.delete_uncommitted(stored)
            self.assertFalse(path.exists())

    def test_delete_uncommitted_rejects_changed_bytes(self) -> None:
        body = b"orphan"
        sha = hashlib.sha256(body).hexdigest()
        artifact_id = uuid4()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FilesystemArtifactStore(temp_dir)
            stored = store.put(artifact_id, body, sha)
            path = Path(stored.storage_uri.removeprefix("file://"))
            path.write_bytes(b"tampered")
            with self.assertRaises(ArtifactIntegrityError):
                store.delete_uncommitted(stored)

    def test_existing_corrupt_object_is_not_silently_reused(self) -> None:
        body = b"expected"
        sha = hashlib.sha256(body).hexdigest()
        artifact_id = uuid4()
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir) / str(artifact_id)
            artifact_dir.mkdir(parents=True)
            (artifact_dir / f"{sha}.bin").write_bytes(b"corrupt")
            store = FilesystemArtifactStore(temp_dir)
            with self.assertRaises(ArtifactIntegrityError):
                store.put(artifact_id, body, sha)


if __name__ == "__main__":
    unittest.main()
