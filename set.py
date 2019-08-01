import redis
import hashlib
import math
from typing import List, Dict


class BitArray(object):

    def __init__(self, size):
        self.size = size
        self.array = bytearray(math.ceil(size / 8))

    def setbit(self, offset, value):
        value = value and 1 or 0
        if math.ceil((offset + 1) / 8) > len(self.array):
            self.array.extend(bytearray(math.ceil((offset + 1) / 8) - len(self.array)))

        array_index = offset // 8
        bits_offset = offset - array_index * 8
        original_byte = self.array[array_index]

        if value == 1:
            byte = original_byte | ((1 << 7) >> bits_offset)
        else:
            byte = original_byte & (~((1 << 7) >> bits_offset))

        self.array[array_index] = byte
        return original_byte == byte

    @property
    def content(self):
        return bytes(self.array)

    def __str__(self):
        return ' '.join(format(x, 'b') for x in self.array)


def string_sub(s: str, begin: int, end: int) -> str:
    return s[begin: end]


def tonumber(s: str, base: int) -> int:
    return int(s, base)


def set_bloom_filter(redis_conn: redis.Redis, key: str, false_positives: float, entries: List[str]):
    count_key: str = f"{key}:count"
    entries_count: int = len(entries)
    factor: int = math.ceil((entries_count + entries_count) / entries_count)
    index: int = math.ceil(math.log(factor) / 0.69314718055995)
    scale = math.pow(2, index - 1) * entries_count
    n_bits: int = math.floor(-(scale * math.log(false_positives * math.pow(0.5, index))) / 0.4804530139182)
    k: int = math.floor(0.69314718055995 * n_bits / scale)

    h: Dict[int, int] = {}
    cnt: int = 0

    bit_array = BitArray(0)

    for entry in entries:
        _hash: str = hashlib.sha1(entry.encode("utf8")).hexdigest()
        h[0] = tonumber(string_sub(_hash, 0, 8), 16)
        h[1] = tonumber(string_sub(_hash, 8, 16), 16)
        h[2] = tonumber(string_sub(_hash, 16, 24), 16)
        h[3] = tonumber(string_sub(_hash, 24, 32), 16)
        found = True
        for i in range(1, k+1):
            found = bit_array.setbit((h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) // 2)]) % n_bits, 1) and found

        if not found:
            cnt += 1
    redis_conn.set(f"{key}:{index}", bit_array.content)
    redis_conn.set(count_key, cnt)
