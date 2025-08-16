import zlib
import msgpack
import enum
import struct
import uuid
from dataclasses import dataclass
from typing import BinaryIO, Generator, Any


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

    def get(self) -> tuple[int, bytes]:
        """
        returns (length of encoded actual data, bytes)
        """
        lsn = struct.pack(">Q", self.lsn)
        transaction = self.transaction_id.bytes
        state = struct.pack("B", self.state.value)
        payload = msgpack.dumps(self.payload)

        data = lsn + transaction + state + payload

        crc = zlib.crc32(data) & 0xFFFFFFFF

        # length = CRC + data
        length = 4 + len(data)

        return length, struct.pack(">I", length) + struct.pack(">I", crc) + data

    @staticmethod
    def parse(buf: bytes) -> "Log":
        return Log._parse_payload(buf[4:])

    @staticmethod
    def parse_buffer(buf: BinaryIO) -> Generator["Log", None, None]:
        while True:
            binary_length = buf.read(4)
            if not binary_length or len(binary_length) < 4:
                break

            length = struct.unpack(">I", binary_length)[0]
            data = buf.read(length)
            if len(data) < length:
                raise EOFError("Incomplete log entry")

            yield Log._parse_payload(data)

    @staticmethod
    def _parse_payload(buf: bytes) -> "Log":
        """
        buf: binary data including CRC and excluding length (4 bytes)
        """
        crc = struct.unpack(">I", buf[:4])[0]

        # The actual data starts after CRC
        data = buf[4:]

        new_crc = zlib.crc32(data) & 0xFFFFFFFF
        if new_crc != crc:
            raise ValueError("Data corrupted")

        lsn = struct.unpack(">Q", data[:8])[0]  # 8 bytes
        transaction_id = uuid.UUID(bytes=data[8:24])  # 16 bytes
        state = State(data[24])  # 1 byte
        payload = msgpack.loads(data[25:])  # rest

        return Log(lsn=lsn, transaction_id=transaction_id, payload=payload, state=state)
