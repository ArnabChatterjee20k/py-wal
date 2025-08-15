from .log import Log
from .segment import SegmentManager
from threading import Lock


# each segment 16mb each
class WAL:
    _last_lsn = 0
    _lock = Lock()

    def __init__(self, directory: str, fsync=False, max_segments=4):
        self._segment_manager = SegmentManager(directory)

    def append():
        pass

    def roatate():
        # sync current segment
        # create new segment
        # delete old segment
        pass

    def sync():
        pass

    def replay():
        pass

    def save_checkpoint():
        pass

    def recover():
        pass
