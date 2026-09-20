"""Immutable local artifact storage for CPI W1 development and tests."""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class StoredArtifact:
    artifact_id: UUID
    sha256: str
    storage_uri: str
    storage_generation: str
    size_bytes: int


class ArtifactIntegrityError(RuntimeError):
    pass


class FilesystemArtifactStore:
    """One immutable object per artifact UUID.

    Different artifact UUIDs never share a deletable object path, even when
    their bytes are identical.
    """

    def __init__(self, data_root: str | Path) -> None:
        self._root = Path(data_root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def _artifact_dir(self, artifact_id: UUID) -> Path:
        return self._root / str(artifact_id)

    @staticmethod
    def _digest(body: bytes) -> str:
        return hashlib.sha256(body).hexdigest()

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        descriptor = os.open(path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def put(self, artifact_id: UUID, body: bytes, sha256: str) -> StoredArtifact:
        if not _SHA256_RE.fullmatch(sha256):
            raise ValueError("sha256 must be lowercase 64-character hex")
        actual = self._digest(body)
        if actual != sha256:
            raise ArtifactIntegrityError("artifact body does not match expected sha256")

        artifact_dir = self._artifact_dir(artifact_id)
        artifact_dir.mkdir(parents=True, exist_ok=True)
        self._fsync_directory(self._root)
        final_path = artifact_dir / f"{sha256}.bin"
        temp_path = artifact_dir / f".tmp-{uuid4()}"

        try:
            with temp_path.open("xb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())

            try:
                os.link(temp_path, final_path)
                self._fsync_directory(artifact_dir)
            except FileExistsError:
                existing = final_path.read_bytes()
                if self._digest(existing) != sha256 or existing != body:
                    raise ArtifactIntegrityError(
                        "existing artifact object does not match expected immutable bytes"
                    )

            stored = final_path.read_bytes()
            if self._digest(stored) != sha256 or stored != body:
                raise ArtifactIntegrityError("stored artifact verification failed")
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

        stat = final_path.stat()
        return StoredArtifact(
            artifact_id=artifact_id,
            sha256=sha256,
            storage_uri=final_path.as_uri(),
            storage_generation=f"inode:{stat.st_ino}:mtime_ns:{stat.st_mtime_ns}",
            size_bytes=stat.st_size,
        )
