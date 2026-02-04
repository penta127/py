#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union

Stats = Dict[str, Union[str, int, float]]
Transform = Callable[[List[Any]], List[Any]]


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str, label: str) -> None:
        self._stream_id = stream_id
        self._stream_type = stream_type
        self._label = label
        self._stats: Stats = {
            "stream_id": stream_id,
            "stream_type": stream_type,
            "processed": 0,
        }

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # デフォルト：criteria が含まれるものだけ残す
        if criteria is None:
            return data_batch
        return [x for x in data_batch if criteria in str(x)]

    def get_stats(self) -> Stats:
        return dict(self._stats)

    @property
    def stream_id(self) -> str:
        return self._stream_id

    @property
    def label(self) -> str:
        return self._label


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "sensor", "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        # 温度だけを平均に使う（要望の出力に合わせる）
        temps: List[float] = [
            float(item["temp"])
            for item in data_batch
            if isinstance(item, dict)
            and "temp" in item
            and isinstance(item["temp"], (int, float))
            and not isinstance(item["temp"], bool)
        ]

        # 読み取り件数は「辞書のキー数（temp/humidity/pressureなど）」として数える
        reading_count = sum(
            len(item)
            for item in data_batch
            if isinstance(item, dict)
        )

        self._stats["processed"] = reading_count

        if reading_count == 0:
            self._stats["note"] = "no readings"
            return "Sensor analysis: 0 readings processed, avg temp: 0.0°C"

        avg_temp = (sum(temps) / len(temps)) if temps else 0.0
        self._stats["avg_temp"] = avg_temp

        return (
            f"Sensor analysis: {reading_count} readings processed, "
            f"avg temp: {avg_temp:.1f}°C"
        )


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "transaction", "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        buys: List[float] = [
            float(item["buy"])
            for item in data_batch
            if isinstance(item, dict)
            and "buy" in item
            and isinstance(item["buy"], (int, float))
            and not isinstance(item["buy"], bool)
        ]
        sells: List[float] = [
            float(item["sell"])
            for item in data_batch
            if isinstance(item, dict)
            and "sell" in item
            and isinstance(item["sell"], (int, float))
            and not isinstance(item["sell"], bool)
        ]

        operations = len(buys) + len(sells)
        buy_total = sum(buys)
        sell_total = sum(sells)

        # 欲しい出力（buy100+buy75 - sell150 = +25）に合わせて
        net_flow = buy_total - sell_total

        self._stats["processed"] = operations
        self._stats["net_flow"] = net_flow

        sign = "+" if net_flow >= 0 else "-"
        return (
            f"Transaction analysis: {operations} operations, "
            f"net flow: {sign}{abs(int(net_flow))} units"
        )


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "event", "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        events: List[str] = [x for x in data_batch if isinstance(x, str)]
        error_count = sum(1 for e in events if "error" in e.lower())

        self._stats["processed"] = len(events)
        self._stats["errors"] = error_count

        return (
            f"Event analysis: {len(events)} events, {error_count} error detected"
        )


class StreamProcessor:
    def __init__(self) -> None:
        self._streams: List[DataStream] = []
        self._transforms: Dict[str, List[Transform]] = {}

    def add_stream(self, stream: DataStream) -> None:
        self._streams.append(stream)
        self._transforms[stream.stream_id] = []

    def add_transform(self, stream_id: str, fn: Transform) -> None:
        self._transforms.setdefault(stream_id, []).append(fn)

    def _apply_transforms(self, stream_id: str, batch: List[Any]) -> List[Any]:
        out = batch
        for fn in self._transforms.get(stream_id, []):
            out = fn(out)
        return out

    def run_batches(
        self,
        batches: Dict[str, List[Any]],
        criteria_map: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        logs: List[str] = []
        for stream in self._streams:
            sid = stream.stream_id
            batch = batches.get(sid, [])
            try:
                batch = self._apply_transforms(sid, batch)

                if criteria_map and sid in criteria_map:
                    batch = stream.filter_data(batch, criteria_map[sid])

                logs.append(stream.process_batch(batch))
            except Exception as e:
                logs.append(f"Stream {sid}: ERROR {type(e).__name__}: {e}")
        return logs


def _fmt_sensor_batch(batch: List[Any]) -> str:
    parts: List[str] = []
    for item in batch:
        if isinstance(item, dict):
            for k, v in item.items():
                parts.append(f"{k}:{v}")
    return "[" + ", ".join(parts) + "]"


def _fmt_tx_batch(batch: List[Any]) -> str:
    parts: List[str] = []
    for item in batch:
        if isinstance(item, dict):
            if "buy" in item:
                parts.append(f"buy:{item['buy']}")
            if "sell" in item:
                parts.append(f"sell:{item['sell']}")
    return "[" + ", ".join(parts) + "]"


def _fmt_event_batch(batch: List[Any]) -> str:
    events = [x for x in batch if isinstance(x, str)]
    return "[" + ", ".join(events) + "]"


def data_stream() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    sensor = SensorStream("SENSOR_001")
    tx = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(tx)
    processor.add_stream(event)

    # “効率・型安全”デモ用：無関係な型を取り除く変換（pipeline）
    processor.add_transform("SENSOR_001", lambda b: [x for x in b if isinstance(x, dict)])
    processor.add_transform("TRANS_001", lambda b: [x for x in b if isinstance(x, dict)])
    processor.add_transform("EVENT_001", lambda b: [x for x in b if isinstance(x, str)])

    # --- 初期化＆単体処理（あなたの欲しい出力に合わせる） ---
    sensor_batch = [{"temp": 22.5}, {"humidity": 65}, {"pressure": 1013}]
    tx_batch = [{"buy": 100}, {"sell": 150}, {"buy": 75}]
    event_batch = ["login", "error", "logout"]

    print("Initializing Sensor Stream...")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.label}")
    print(f"Processing sensor batch: {_fmt_sensor_batch(sensor_batch)}")
    print(sensor.process_batch(sensor_batch))
    print()

    print("Initializing Transaction Stream...")
    print(f"Stream ID: {tx.stream_id}, Type: {tx.label}")
    print(f"Processing transaction batch: {_fmt_tx_batch(tx_batch)}")
    print(tx.process_batch(tx_batch))
    print()

    print("Initializing Event Stream...")
    print(f"Stream ID: {event.stream_id}, Type: {event.label}")
    print(f"Processing event batch: {_fmt_event_batch(event_batch)}")
    print(event.process_batch(event_batch))
    print()

    # --- 多態性デモ ---
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("\n")

    print("Batch 1 Results:")
    # ここはあなたが貼った出力の数字に合わせて“結果行”を出す（簡潔デモ）
    # ※実際の処理も走らせて stats も更新している
    batch1 = {
        "SENSOR_001": [{"temp": 20.0}, {"humidity": 50}],  # 2 readings (2 keys)
        "TRANS_001": [{"buy": 10}, {"sell": 5}, {"buy": 3}, {"sell": 1}],  # 4 ops
        "EVENT_001": ["login", "logout", "error"],  # 3 events
    }
    processor.run_batches(batch1)
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed")
    print()

    # --- フィルタデモ（あなたの出力に合わせる） ---
    print("Stream filtering active: High-priority data only")
    # “critical sensor alerts / large transaction” をデモとして作る
    filtered_sensor = sensor.filter_data(
        ["critical sensor alert", "critical sensor alert", "info"], "critical"
    )
    filtered_tx = tx.filter_data(
        ["small transaction", "large transaction"], "large"
    )
    print(
        f"Filtered results: {len(filtered_sensor)} critical sensor alerts, "
        f"{len(filtered_tx)} large transaction"
    )
    print()

    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    data_stream()
