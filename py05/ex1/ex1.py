from abc import ABC,abstractmethod
from typing import Any, Dict, List, Optional, Union

stats = Dict[str, Union[str, int, float]]

class Data_Stream(ABC):

    def __init__(self, id: str, type: str) -> None:
        self._id = id
        self._type = type
        self._stats: stats ={
            "id": id,
            "type": type,
            "processed": 0,
        }

    @abstractmethod
    def process_batch(self, data_batch: list[Any]) -> str:
        raise NotImplementedError

    def filter(self, data_batch: list[Any], criteria: Optional[str] = None) -> list[Any]:
        if criteria is None:
            return data_batch

        return [x for x in data_batch if criteria in str(x)]

    def get_stats(self) -> stats:
        return dict(self._stats)


class SensorStream(Data_Stream):
    def __init__(self, id: str) -> None:
        super().__init__(id, "sensor")

        def process_batch(self, data_batch: list[Any]) -> str:
            readings: list[float] = []
            for item in data_batch:
                if isinstance(item, dict):
                    for v in item.values():
                        if isinstance(v,(int, float)) and not isinstance(v, bool):
                            readings.append(float(v))
            if not readings:
                self._stats["processed"] = 0
                self._stats["note"] = "no numeric readings"
                return f"Stream {self._id}: no valid sensor readings"

                count = len(readings)
                avg = sum(readings) / count

                self._stats["processed"] = count
                self._stats["avg"] = avg
                self._stats["min"] = min(readings)
                self._stats["max"] = max(readings)

                return f"Stream {self._stream_id}: {count} readings processed, avg={avg}"


class TransactionStream(DataStream):

    def __init__(self, id: str) -> None:
        super().__init__(id, "transaction")

    def process_batch(self, data_batch: list[Any]) -> str:
        buy_total = 0.0
        sell_total = 0.0

        for item in data_batch:
            if not isinstance(item, dict):
                continue
            buy = item.get("buy")
            sell = item.get("sell")
            if isinstance(buy, (int, float)) and not isinstance(buy, bool):
                buy_total += float(buy)
            if isinstance(sell, (int, float)) and not isinstance(sell, bool):
                sell_total += float(sell)

        processed = 0

        for item in data_batch:
            if isinstance(item, dict) and ("buy" in item or "sell" in item):
                processed += 1

        net = sell_total - buy_total

        self._stats["processed"] = processed
        self._stats["buy_total"] = buy_total
        self._stats["sell_total"] = sell_total
        self._stats["net"] = net

        return (
            f"Stream {self._stream_id}: {processed} tx processed, net={net}"
        )


def data_stream() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

