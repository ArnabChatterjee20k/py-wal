import zlib
import msgpack
import enum
import struct
import uuid
from dataclasses import dataclass, field


class State(enum.Enum):
    BEGIN = 0
    COMMIT = 1
    ABORT = 2


"""
CRC = algo(LSN,Data) => crc is calculated on binary data

taking transaction id as uuid -> 16bytes
commit flags -> 1byte
for ease of parsing following this order
[Length (4B)][CRC (4B)] [LSN (8B)][TxnID (16B)][State (1B)][Payload...(variable size)]
data = [LSN (8B)][TxnID (16B)][State (1B)][Payload...(variable size)]
"""


@dataclass
class Log:
    lsn: int
    transaction_id: uuid.UUID
    payload: dict
    state: State = State.BEGIN
    length: int = field(init=False, default=4)

    def get(self):
        lsn = struct.pack(">Q", self.lsn)
        transaction = self.transaction_id.bytes
        state = struct.pack("B", self.state.value)
        payload = msgpack.dumps(self.payload)

        data = lsn + transaction + state + payload

        crc = zlib.crc32(data) & 0xFFFFFFFF

        # length = CRC + data
        length = 4 + len(data)

        return struct.pack(">I", length) + struct.pack(">I", crc) + data

    @staticmethod
    def parse(buf) -> "Log":
        length, crc = struct.unpack(">II", buf[:8])

        data = buf[8:]
        new_crc = zlib.crc32(data) & 0xFFFFFFFF
        if new_crc != crc:
            raise ValueError("Data corrupted")

        lsn = struct.unpack(">Q", buf[8:16])[0]
        transaction_id = uuid.UUID(bytes=buf[16:32])
        # state = struct.unpack("B",buf[32])[0]
        state = State(buf[32])
        payload = msgpack.loads(buf[33:])

        return Log(lsn=lsn, transaction_id=transaction_id, payload=payload, state=state)
