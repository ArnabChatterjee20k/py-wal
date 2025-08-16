import shutil, glob, io, os
from pathlib import Path


class Segment:
    def __init__(self, path: Path):
        self._path = path
        self._file = open(self._path, "ab")
        self._file.seek(0, io.SEEK_END)

    def append(self, data: bytes):
        self._file.write(data)

    def read(self, offset):
        pass

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
        if not self.get_last_segment().exists():
            self.create()

    def create(self) -> Segment:
        name = self._format_name(self._last_segment)
        path = self._source_dir / f"{name}.{self._extension}"
        path.touch()
        self._last_segment += 1
        return Segment(path)

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
