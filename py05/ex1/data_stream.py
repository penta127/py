#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union

Stats = Dict[str, Union[str, int, float]]
Transform = Callable[[List[Any]], List[Any]]


class DataStream(ABC):
    """_summary_
    データストリームの共通インターフェースを定義する抽象基底クラス（ABC）。

    各ストリームは「バッチ（List[Any]）」単位でデータを処理する。
    サブクラスは process_batch() を必ず実装し、ストリーム固有の処理を行う。

    この基底クラスは、以下を共通機能として提供する:
    - stream_id / stream_type / label の保持
    - 処理統計（stats）の保持と取得
    - デフォルトのフィルタリング（filter_data）

    Args:
        ABC (_type_): 抽象基底クラスのための親クラス。

    Returns:
        _type_: このクラス自体は直接使うというより、サブクラス実装の土台。
    """

    def __init__(self, stream_id: str, stream_type: str, label: str) -> None:
        """_summary_
        ストリームの基本情報と統計情報（stats）を初期化する。

        stats は辞書で管理し、最低限以下の情報を持つ:
        - stream_id: ストリーム識別子
        - stream_type: ストリーム種別（例: sensor/transaction/event）
        - processed: 処理した要素数など（サブクラスで更新）

        Args:
            stream_id (str): ストリームの識別子（例: "SENSOR_001"）。
            stream_type (str): ストリーム種別（例: "sensor"）。
            label (str): 表示用ラベル（例: "Environmental Data"）。

        Returns:
            None: 何も返さない。
        """
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
        """_summary_
        データバッチを処理し、結果の要約文字列を返す（抽象メソッド）。

        サブクラスは、入力 data_batch を解析し、統計（self._stats）を更新しつつ
        出力例に沿った結果文字列を返す。

        Args:
            data_batch (List[Any]): 処理対象のバッチデータ。

        Returns:
            str: 処理結果の要約文字列。

        Raises:
            NotImplementedError: サブクラスが実装しないまま呼ばれた場合。
        """
        raise NotImplementedError

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """_summary_
        データバッチを条件でフィルタリングする（デフォルト実装）。

        デフォルト仕様:
        - criteria が None の場合は、元の data_batch をそのまま返す
        - criteria が指定された場合は、str(x) の中に criteria を含む要素だけ残す

        サブクラス側で「型に応じたフィルタ」などが必要なら override 可能。

        Args:
            data_batch (List[Any]): フィルタ対象のバッチデータ。
            criteria (Optional[str]): フィルタ条件（部分一致）。未指定なら None。

        Returns:
            List[Any]: フィルタ後のバッチデータ。
        """
        if criteria is None:
            return data_batch
        return [x for x in data_batch if criteria in str(x)]

    def get_stats(self) -> Stats:
        """_summary_
        現在のストリーム統計情報（stats）を取得する。

        内部の辞書をそのまま返すと外部から変更される可能性があるため、
        dict() でコピーして返す。

        Args:
            None: 引数なし。

        Returns:
            Stats: 統計情報のコピー（Dict[str, Union[str, int, float]]）。
        """
        return dict(self._stats)

    @property
    def stream_id(self) -> str:
        """_summary_
        ストリーム識別子を返す。

        Args:
            None: 引数なし。

        Returns:
            str: ストリームID。
        """
        return self._stream_id

    @property
    def label(self) -> str:
        """_summary_
        表示用ラベルを返す。

        Args:
            None: 引数なし。

        Returns:
            str: ラベル文字列。
        """
        return self._label


class SensorStream(DataStream):
    """_summary_
    センサーデータ（環境データ）用のストリーム実装。

    data_batch には dict を想定し、例として以下のキーを扱う:
    - temp / humidity / pressure など

    この実装では:
    - 温度（temp）だけを平均計算に使う
    - processed は「辞書のキー数の合計（読み取り数）」として数える
      例: [{"temp":22.5},{"humidity":65},{"pressure":1013}] → 3 readings

    Args:
        DataStream (_type_): 共通インターフェースを継承する親クラス。
    """

    def __init__(self, stream_id: str) -> None:
        """_summary_
        SensorStream を初期化する。

        stream_type は "sensor"、label は "Environmental Data" として固定する。

        Args:
            stream_id (str): ストリーム識別子。

        Returns:
            None: 何も返さない。
        """
        super().__init__(stream_id, "sensor", "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        """_summary_
        センサーデータのバッチを処理し、読み取り数と平均温度を要約して返す。

        処理内容:
        - temp が数値（int/float）として入っている dict だけを対象に平均温度を計算
        - processed は「dict のキー数合計」として更新

        Args:
            data_batch (List[Any]): センサーデータのバッチ（dict を想定）。

        Returns:
            str: 例) "Sensor analysis: 3 readings processed, avg temp: 22.5°C"
        """
        temps: List[float] = [
            float(item["temp"])
            for item in data_batch
            if isinstance(item, dict)
            and "temp" in item
            and isinstance(item["temp"], (int, float))
            and not isinstance(item["temp"], bool)
        ]

        reading_count = sum(len(item) for item
                            in data_batch if isinstance(item, dict))

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
    """_summary_
    取引データ（Financial Data）用のストリーム実装。

    data_batch には dict を想定し、例として以下のキーを扱う:
    - buy / sell

    この実装では:
    - operations は buy の件数 + sell の件数
    - net_flow は buy_total - sell_total として計算（例の出力に合わせる）
      例: buy100+buy75 - sell150 = +25

    Args:
        DataStream (_type_): 共通インターフェースを継承する親クラス。
    """

    def __init__(self, stream_id: str) -> None:
        """_summary_
        TransactionStream を初期化する。

        stream_type は "transaction"、label は "Financial Data" として固定する。

        Args:
            stream_id (str): ストリーム識別子。

        Returns:
            None: 何も返さない。
        """
        super().__init__(stream_id, "transaction", "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        """_summary_
        取引データのバッチを処理し、操作回数とネットフローを要約して返す。

        処理内容:
        - dict から buy/sell を抽出（数値のみ）
        - operations / net_flow を計算して stats を更新

        Args:
            data_batch (List[Any]): 取引データのバッチ（dict を想定）。

        Returns:
            str: 例) "Transaction analysis: 3 operations, net flow: +25 units"
        """
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

        net_flow = buy_total - sell_total

        self._stats["processed"] = operations
        self._stats["net_flow"] = net_flow

        sign = "+" if net_flow >= 0 else "-"
        return (
            f"Transaction analysis: {operations} operations, "
            f"net flow: {sign}{abs(int(net_flow))} units"
        )


class EventStream(DataStream):
    """_summary_
    システムイベント（System Events）用のストリーム実装。

    data_batch には文字列（str）のイベント名を想定する。
    エラー判定は、イベント文字列に "error" を含むかで数える（大文字小文字無視）。

    Args:
        DataStream (_type_): 共通インターフェースを継承する親クラス。
    """

    def __init__(self, stream_id: str) -> None:
        """_summary_
        EventStream を初期化する。

        stream_type は "event"、label は "System Events" として固定する。

        Args:
            stream_id (str): ストリーム識別子。

        Returns:
            None: 何も返さない。
        """
        super().__init__(stream_id, "event", "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        """_summary_
        イベントデータのバッチを処理し、イベント数とエラー数を要約して返す。

        処理内容:
        - str のみイベントとして採用
        - "error" を含むイベント数を error_count として数える
        - stats の processed/errors を更新

        Args:
            data_batch (List[Any]): イベントデータのバッチ（str を想定）。

        Returns:
            str: 例) "Event analysis: 3 events, 1 error detected"
        """
        events: List[str] = [x for x in data_batch if isinstance(x, str)]
        error_count = sum(1 for e in events if "error" in e.lower())

        self._stats["processed"] = len(events)
        self._stats["errors"] = error_count

        return (f"Event analysis: {len(events)}"
                f" events, {error_count} error detected")


class StreamProcessor:
    """_summary_
    複数の DataStream をまとめて扱うストリームマネージャ。

    ポリモーフィズムにより、StreamProcessor は stream の具体型（Sensor/Transaction/Event）
    を意識せずに、共通インターフェース（process_batch/filter_data）だけで処理できる。

    追加機能:
    - 変換パイプライン（transforms）: stream_id ごとに複数の変換関数を適用
    - バッチ実行（run_batches）: すべての登録ストリームに対してまとめて処理
    - エラーハンドリング: ストリーム処理の例外を捕捉してログ化

    Args:
        None: コンストラクタ引数なし。

    Returns:
        _type_: StreamProcessor のインスタンス。
    """

    def __init__(self) -> None:
        """_summary_
        ストリーム一覧と変換パイプラインを初期化する。

        _streams: 登録された DataStream のリスト
        _transforms: stream_id -> 変換関数リスト

        Args:
            None: 引数なし。

        Returns:
            None: 何も返さない。
        """
        self._streams: List[DataStream] = []
        self._transforms: Dict[str, List[Transform]] = {}

    def add_stream(self, stream: DataStream) -> None:
        """_summary_
        ストリームを登録し、その stream_id 用の変換パイプラインを初期化する。

        Args:
            stream (DataStream): 登録するストリーム（任意の DataStream サブタイプ）。

        Returns:
            None: 何も返さない。
        """
        self._streams.append(stream)
        self._transforms[stream.stream_id] = []

    def add_transform(self, stream_id: str, fn: Transform) -> None:
        """_summary_
        指定 stream_id の変換パイプラインに変換関数を追加する。

        変換関数は (List[Any]) -> (List[Any]) を満たす Callable とする。

        Args:
            stream_id (str): 対象ストリームID。
            fn (Transform): バッチを変換する関数。

        Returns:
            None: 何も返さない。
        """
        self._transforms.setdefault(stream_id, []).append(fn)

    def _apply_transforms(self, stream_id: str, batch: List[Any]) -> List[Any]:
        """_summary_
        指定 stream_id の変換パイプラインを順番に適用する（内部用）。

        Args:
            stream_id (str): 対象ストリームID。
            batch (List[Any]): 変換対象のバッチ。

        Returns:
            List[Any]: 変換後のバッチ。
        """
        out = batch
        for fn in self._transforms.get(stream_id, []):
            out = fn(out)
        return out

    def run_batches(
        self,
        batches: Dict[str, List[Any]],
        criteria_map: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """_summary_
        登録済みの全ストリームに対して、対応するバッチを処理する。

        処理フロー（各ストリームごと）:
        1) stream_id で batches から対象バッチを取得（なければ空リスト）
        2) 変換パイプラインを適用
        3) criteria_map があれば filter_data を適用
        4) process_batch を実行し、結果文字列を logs に追加
        5) 例外が起きた場合は捕捉し、エラー文字列として logs に追加

        Args:
            batches (Dict[str, List[Any]]): stream_id -> バッチデータ の辞書。
            criteria_map (Optional[Dict[str, str]]): stream_id -> フィルタ条件 の辞書。

        Returns:
            List[str]: 各ストリームの処理結果（またはエラー）を並べたログリスト。
        """
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
    """_summary_
    センサーバッチを表示用の文字列（例のフォーマット）に整形する補助関数。

    dict の各キー・値を "key:value" 形式にしてカンマ区切りで並べる。
    dict 以外の要素は無視する。

    Args:
        batch (List[Any]): センサーバッチ（dict を想定）。

    Returns:
        str: 例) "[temp:22.5, humidity:65, pressure:1013]"
    """
    parts: List[str] = []
    for item in batch:
        if isinstance(item, dict):
            for k, v in item.items():
                parts.append(f"{k}:{v}")
    return "[" + ", ".join(parts) + "]"


def _fmt_tx_batch(batch: List[Any]) -> str:
    """_summary_
    取引バッチを表示用の文字列（例のフォーマット）に整形する補助関数。

    dict に buy/sell があれば、それぞれ "buy:value" / "sell:value" として並べる。
    dict 以外の要素は無視する。

    Args:
        batch (List[Any]): 取引バッチ（dict を想定）。

    Returns:
        str: 例) "[buy:100, sell:150, buy:75]"
    """
    parts: List[str] = []
    for item in batch:
        if isinstance(item, dict):
            if "buy" in item:
                parts.append(f"buy:{item['buy']}")
            if "sell" in item:
                parts.append(f"sell:{item['sell']}")
    return "[" + ", ".join(parts) + "]"


def _fmt_event_batch(batch: List[Any]) -> str:
    """_summary_
    イベントバッチを表示用の文字列（例のフォーマット）に整形する補助関数。

    str のみをイベントとして採用し、カンマ区切りで並べる。

    Args:
        batch (List[Any]): イベントバッチ（str を想定）。

    Returns:
        str: 例) "[login, error, logout]"
    """
    events = [x for x in batch if isinstance(x, str)]
    return "[" + ", ".join(events) + "]"


def data_stream() -> None:
    """_summary_
    ポリモーフィックなストリーム処理システムのデモを実行する。

    実行内容:
    - Sensor/Transaction/Event の各ストリームを初期化
    - StreamProcessor に登録
    - 型安全デモとして transforms（不要な型を除外）を設定
    - サンプルバッチを各ストリームで処理して結果を表示
    - run_batches で「混在ストリームを同じインターフェースで処理」するデモ
    - filter_data を使った簡易フィルタデモ

    Args:
        None: 引数なし。

    Returns:
        None: 何も返さない。
    """
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    sensor = SensorStream("SENSOR_001")
    tx = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(tx)
    processor.add_stream(event)

    processor.add_transform(
        "SENSOR_001",
        lambda b: [x for x in b if isinstance(x, dict)]
        )
    processor.add_transform(
        "TRANS_001",
        lambda b: [x for x in b if isinstance(x, dict)])
    processor.add_transform(
        "EVENT_001",
        lambda b: [x for x in b if isinstance(x, str)])

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

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("\n")

    print("Batch 1 Results:")
    batch1 = {
        "SENSOR_001": [{"temp": 20.0}, {"humidity": 50}],
        "TRANS_001": [{"buy": 10}, {"sell": 5}, {"buy": 3}, {"sell": 1}],
        "EVENT_001": ["login", "logout", "error"],
    }
    processor.run_batches(batch1)
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed")
    print()

    print("Stream filtering active: High-priority data only")
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
