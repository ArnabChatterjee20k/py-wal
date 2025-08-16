from .log import Log, State
from .segment import SegmentManager
from threading import Lock, Timer
import uuid


# each segment 16mb each
class WAL:

    def __init__(
        self, directory: str, fsync=False, max_segments=4, sync_interval_ms=200
    ):
        self._segment_manager = SegmentManager(directory)
        self._fsync = fsync

        self._last_lsn = 0
        self._lock = Lock()

        self.sync_interval_ms = sync_interval_ms
        self._timer: Timer = None
        self._running = True

        self.start_syncing()

    @property
    def _buffer(self):
        """we dont need an external buffer, we can write to the buffer of the segment/file stream"""
        return self._segment_manager.get_last_segment()

    def append(
        self,
        entry,
        transaction_id: uuid.UUID = uuid.uuid4(),
        state: State = State.BEGIN,
    ):
        with self._lock:
            self.roatate()
            log = Log(
                lsn=self._last_lsn,
                transaction_id=transaction_id,
                payload=entry,
                state=state,
            )
            length, entry = log.get()
            # TODO: check if new segment required
            self._buffer.append(entry)

    def close(self):
        self._running = False
        self.sync()
        if self._timer:
            self._timer.join()
        self._buffer.close()

    def roatate(self):
        # sync current segment
        # rotate
        # write to segment
        pass

    def replay(self):
        pass

    def save_checkpoint(self):
        # checkpoints can be simulated via the entry itself
        pass

    def recover(self):
        pass

    # timer related (to have control over manual sync and auto sync we are triggering a sync and reseting)
    def sync(self):
        """for manual syncing of buffer"""
        self._buffer.flush()
        # immediately write to the disk
        if self._fsync:
            self._buffer.fsync()
        self._reset_timer()

    def start_syncing(self):
        """to use sync() at regular interval"""
        self._schedule_timer()

    def _sync_worker(self):
        """for syncing in a threadsafe manner"""
        with self.lock:
            self.sync()

    def _schedule_timer(self):
        # self._timer = Timer(self.sync_interval_ms,self.sync) # without external locking
        self._timer = Timer(self.sync_interval_ms, self._sync_worker)
        self._timer.start()

    def _reset_timer(self):
        if self._timer:
            self._timer.cancel()
        # reschedule the timer
        if self._running:
            self._schedule_timer()
