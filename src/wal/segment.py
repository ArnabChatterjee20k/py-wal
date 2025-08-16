import shutil, glob, io, os
from pathlib import Path


class Segment:
    def __init__(self, path: Path):
        self._path = path
        self._file = open(self._path, "ab")
        self._file.seek(0, io.SEEK_END)
        self._file_read = None

    def __enter__(self):
        self._read()
        return self._file_read

    def __exit__(self, *args, **kwargs):
        self._file_read.close()

    def append(self, data: bytes):
        self._file.write(data)

    def _read(self):
        self._file_read = open(self._path, "rb")

    def get_size(self):
        return os.stat(self._path).st_size

    def flush(self):
        self._file.flush()

    def fsync(self):
        os.fsync(self._file.fileno())

    def exists(self):
        return self._path.exists()

    def close(self):
        self._file.flush()
        self._file.close()


class SegmentManager:
    def __init__(self, source: str, extension="log"):
        self._source_dir = Path(source)
        self._extension = extension
        self._last_segment = 0
        if not self._source_dir.exists():
            self._source_dir.mkdir(parents=True, exist_ok=True)

    def create(self):
        name = self._format_name(self._last_segment)
        path = self._source_dir / f"{name}.{self._extension}"
        path.touch()
        self._last_segment += 1

    def get_segment(self, name: str) -> Segment:
        path = self._source_dir / f"{name}.{self._extension}"
        return Segment(path)

    def get_last_segment(self) -> Segment:
        name = self._format_name(self._last_segment)
        path = self._source_dir / f"{name}.{self._extension}"
        return Segment(path)

    def list_segments(self) -> list[Segment]:
        return [Segment(path) for path in self._source_dir.iterdir() if path.is_file()]

    def _format_name(self, name: str):
        # 8 bytes
        return f"{name:08d}"
