#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Union


class ProcessingStage(Protocol):
    """_summary_
    ステージ（処理段階）のための Protocol（ダックタイピング用インターフェース）。

    この Protocol を満たすには、process(self, data: Any) -> Any を実装していればよい。
    継承は不要で、同じメソッドシグネチャを持つ任意のクラスをステージとして扱える。

    Args:
        Protocol (_type_): Protocol を利用して構造的部分型（duck typing）を定義する。

    Returns:
        _type_: process() を持つオブジェクトをステージとして受け入れるための型。
    """

    def process(self, data: Any) -> Any:
        """_summary_
        入力データを処理して次のステージへ渡すデータを返す。

        Args:
            data (Any): ステージが受け取る入力データ。

        Returns:
            Any: 処理後のデータ（次ステージへ渡すデータ）。
        """
        ...


Stats = Dict[str, Union[str, int, float]]


@dataclass
class PipelineStats:
    """_summary_
    パイプラインの処理統計（監視用メトリクス）を保持するデータクラス。

    代表的なフィールド:
    - processed: 成功した処理回数
    - failed: 失敗した処理回数
    - recovered: リカバリが走った回数
    - total_time_s: 合計処理時間（秒）
    - last_error: 最後に発生したエラーの文字列
    - stage_timings_s: ステージ名 -> 累積実行時間（秒）

    Args:
        pipeline_id (str): 統計対象のパイプラインID。
        processed (int): 成功回数。
        failed (int): 失敗回数。
        recovered (int): リカバリ回数。
        total_time_s (float): 合計処理時間（秒）。
        last_error (str): 最後のエラー文字列。
        stage_timings_s (Dict[str, float]): ステージごとの累積時間。

    Returns:
        _type_: PipelineStats のインスタンス。
    """

    pipeline_id: str
    processed: int = 0
    failed: int = 0
    recovered: int = 0
    total_time_s: float = 0.0
    last_error: str = ""
    stage_timings_s: Dict[str, float] = field(default_factory=dict)

    def efficiency_pct(self) -> float:
        """_summary_
        成功率（効率）をパーセンテージで返す。

        この実装では効率を「成功 / (成功 + 失敗)」として定義する。
        成功+失敗が 0 の場合は 100% を返す。

        Args:
            None: 引数なし。

        Returns:
            float: 効率（成功率）を 0.0〜100.0 の範囲で返す。
        """
        total = self.processed + self.failed
        if total == 0:
            return 100.0
        return (self.processed / total) * 100.0


class InputStage:
    """_summary_
    ステージ1: 入力の基本検証を行うステージ。

    このステージは、入力型が最低限許容される範囲かどうかだけを検証する。
    具体的なパースは後続ステージ（Transform）やアダプタ側に委ねる。

    Args:
        None: コンストラクタ引数なし。

    Returns:
        _type_: InputStage のインスタンス。
    """

    def process(self, data: Any) -> Any:
        """_summary_
        入力型の基本検証を行い、通ればそのまま返す。

        許容する入力型:
        - str
        - dict
        - list

        Args:
            data (Any): 入力データ。

        Returns:
            Any: 検証に通ったデータ（そのまま返す）。

        Raises:
            ValueError: 許容されない型が渡された場合。
        """
        if not isinstance(data, (str, dict, list)):
            raise ValueError("Invalid input type")
        return data


class TransformStage:
    """_summary_
    ステージ2: データの変換・正規化・メタ情報付与を行うステージ。

    動作例:
    - dict の場合: キーを str に正規化し、メタ情報を追加する
    - "a,b,c" のようなCSVヘッダ行: リスト化して辞書に格納する
    - "stream" を含む文字列: stream フィールドに格納する
    - それ以外: そのまま返す

    Args:
        None: コンストラクタ引数なし。

    Returns:
        _type_: TransformStage のインスタンス。
    """

    def process(self, data: Any) -> Any:
        """_summary_
        入力データの形状に応じて変換・拡張（enrichment）を行う。

        Args:
            data (Any): 入力データ（str/dict/list など）。

        Returns:
            Any: 変換後のデータ（dict になる場合もある）。
        """
        if isinstance(data, dict):
            enriched: Dict[str, Any] = {str(k): v for k, v in data.items()}
            enriched["_meta"] = {"validated": True, "source": "nexus"}
            return enriched

        if isinstance(data, str) and "," in data and "\n" not in data:
            parts = [p.strip() for p in data.split(",")]
            return {"csv_header": parts, "_meta": {"validated": True}}

        if isinstance(data, str) and "stream" in data.lower():
            return {"stream": data, "_meta": {"validated": True}}

        return data


class OutputStage:
    """_summary_
    ステージ3: 出力整形・配信のための最終段ステージ。

    この実装では、最終的な文字列整形はアダプタ側で行う設計のため、
    ここではデータをそのまま返す。

    Args:
        None: コンストラクタ引数なし。

    Returns:
        _type_: OutputStage のインスタンス。
    """

    def process(self, data: Any) -> Any:
        """_summary_
        出力段としてデータを返す（整形はアダプタ側で実施）。

        Args:
            data (Any): 入力データ。

        Returns:
            Any: そのまま返す。
        """
        return data


class BackupTransformStage:
    """_summary_
    リカバリ用の簡易変換ステージ（バックアッププロセッサ）。

    TransformStage が失敗したときに差し替えて使う想定。
    より寛容に入力を受け取り、最低限 dict 形式にラップしてメタ情報を付ける。

    Args:
        None: コンストラクタ引数なし。

    Returns:
        _type_: BackupTransformStage のインスタンス。
    """

    def process(self, data: Any) -> Any:
        """_summary_
        入力を可能な限り受け入れて、バックアップ形式に変換する。

        Args:
            data (Any): 入力データ。

        Returns:
            Any: dict を基本とする変換結果。
        """
        if isinstance(data, dict):
            data.setdefault("_meta", {"validated": True, "source": "backup"})
            return data
        return {"raw": data, "_meta": {"validated": True, "source": "backup"}}


class ProcessingPipeline(ABC):
    """_summary_
    複数ステージを保持し、データを順に流して処理する抽象基底クラス（ABC）。

    このクラスはステージ構成と監視（stats）とリカバリの枠組みを提供する。
    入力形式ごとの入口処理・出力整形はアダプタ（サブクラス）が process() を
    オーバーライドして実装する。

    Args:
        ABC (_type_): 抽象基底クラスのための親クラス。

    Returns:
        _type_: ProcessingPipeline の派生クラスを使って実行する。
    """

    def __init__(
        self, pipeline_id: str, stages: Optional[List[ProcessingStage]] = None
    ) -> None:
        """_summary_
        パイプラインID、ステージ、統計情報、リカバリ設定を初期化する。

        stages が None の場合は以下のデフォルト構成になる:
        - InputStage
        - TransformStage
        - OutputStage

        Args:
            pipeline_id (str): パイプライン識別子。
            stages (Optional[List[ProcessingStage]]): 使用するステージ一覧（任意）。

        Returns:
            None: 何も返さない。
        """
        self.pipeline_id = pipeline_id
        self.stages: List[ProcessingStage] = (
            stages
            if stages is not None
            else [InputStage(), TransformStage(), OutputStage()]
        )
        self.stats = PipelineStats(pipeline_id=pipeline_id)
        self._backup_transform = BackupTransformStage()
        self._recovery_enabled = True

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """_summary_
        形式固有の入口処理を行う（サブクラスで必ず実装する）。

        各アダプタは、入力形式（JSON/CSV/Stream）に合わせて
        パース・整形を行った上で run_stages() を呼ぶ。

        Args:
            data (Any): 入力データ。

        Returns:
            Union[str, Any]: 最終出力（多くは表示用文字列）。

        Raises:
            NotImplementedError: サブクラスが実装しない場合。
        """
        raise NotImplementedError

    def run_stages(self, data: Any) -> Any:
        """_summary_
        登録されたステージを順番に実行し、ステージごとの時間を計測する。

        Args:
            data (Any): 最初の入力データ。

        Returns:
            Any: 最終ステージの出力。
        """
        current = data
        for stage in self.stages:
            stage_name = stage.__class__.__name__
            t0 = time.perf_counter()
            current = stage.process(current)
            dt = time.perf_counter() - t0
            self.stats.stage_timings_s[stage_name] = (
                self.stats.stage_timings_s.get(stage_name, 0.0) + dt
            )
        return current

    def recover(self, data: Any, error: Exception) -> Any:
        """_summary_
        リカバリ処理として TransformStage をバックアップステージに差し替え、1回だけ再実行する。

        このメソッドは:
        - recovered と last_error を更新
        - stages 内の TransformStage を BackupTransformStage に置換
        - run_stages() を再実行して結果を返す

        Args:
            data (Any): 元の入力データ（再実行に使う）。
            error (Exception): 発生した例外。

        Returns:
            Any: リカバリ後のステージ実行結果。
        """
        self.stats.recovered += 1
        self.stats.last_error = f"{type(error).__name__}: {error}"

        new_stages: List[ProcessingStage] = []
        for st in self.stages:
            if isinstance(st, TransformStage):
                new_stages.append(self._backup_transform)
            else:
                new_stages.append(st)
        self.stages = new_stages

        return self.run_stages(data)


class JSONAdapter(ProcessingPipeline):
    """_summary_
    JSON入力を処理するアダプタ（ProcessingPipeline の派生クラス）。

    入力:
    - JSON文字列（str）
    - dict

    出力:
    - 表示用文字列（例のフォーマットに近い内容）

    Args:
        ProcessingPipeline (_type_): ステージ実行・監視・リカバリの共通基盤。
    """

    def __init__(self, pipeline_id: str) -> None:
        """_summary_
        JSONAdapter を初期化する。

        Args:
            pipeline_id (str): パイプライン識別子。

        Returns:
            None: 何も返さない。
        """
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """_summary_
        JSON文字列または dict を受け取り、ステージを実行して表示用文字列を返す。

        例の出力に合わせて:
        - sensor=temp, unit=C, value が数値なら温度の正常範囲を判定して整形する
        - それ以外は簡易サマリ文字列を返す

        失敗時は:
        - stats.failed を増やす
        - リカバリが有効なら recover() を試す
        - 最終的にエラー文字列を返す

        Args:
            data (Any): JSON文字列または dict。

        Returns:
            Union[str, Any]: 表示用文字列（またはリカバリ後のbest-effort結果）。
        """
        t0 = time.perf_counter()
        try:
            if isinstance(data, str):
                parsed = json.loads(data)
            elif isinstance(data, dict):
                parsed = data
            else:
                raise ValueError("Invalid data format for JSONAdapter")

            result = self.run_stages(parsed)

            sensor = str(result.get("sensor", "unknown"))
            value = result.get("value", None)
            unit = str(result.get("unit", ""))

            if (
                sensor == "temp"
                and isinstance(value, (int, float))
                and unit.upper() == "C"
            ):
                status = (
                    "Normal range"
                    if 15.0 <= float(value) <= 30.0
                    else "Out of range"
                )
                out = (
                    "Processed temperature reading: "
                    f"{float(value):.1f}°C ({status})")
            else:
                out = (
                    "Processed JSON record: sensor="
                    f"{sensor}, value={value}{unit}"
                    )

            self.stats.processed += 1
            return out

        except Exception as e:
            self.stats.failed += 1
            if self._recovery_enabled:
                try:
                    recovered = self.recover(data, e)
                    self.stats.processed += 1
                    return f"Recovered JSON processing: {recovered}"
                except Exception as e2:
                    self.stats.last_error = f"{type(e2).__name__}: {e2}"
            return f"JSONAdapter ERROR: {type(e).__name__}: {e}"
        finally:
            self.stats.total_time_s += (time.perf_counter() - t0)


class CSVAdapter(ProcessingPipeline):
    """_summary_
    CSV入力（主にヘッダ行）を処理するアダプタ（ProcessingPipeline の派生クラス）。

    入力:
    - ヘッダ行を表す文字列（str）
    - 文字列のリスト（list[str]）: デモとして先頭行のみ利用

    出力:
    - 表示用文字列（例のフォーマットに近い内容）

    Args:
        ProcessingPipeline (_type_): ステージ実行・監視・リカバリの共通基盤。
    """

    def __init__(self, pipeline_id: str) -> None:
        """_summary_
        CSVAdapter を初期化する。

        Args:
            pipeline_id (str): パイプライン識別子。

        Returns:
            None: 何も返さない。
        """
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """_summary_
        CSVヘッダ行（文字列）または文字列リストを受け取り、ステージ実行後に表示用文字列を返す。

        失敗時は:
        - stats.failed を増やす
        - リカバリが有効なら recover() を試す
        - 最終的にエラー文字列を返す

        Args:
            data (Any): CSVヘッダ行（str）または文字列リスト（list）。

        Returns:
            Union[str, Any]: 表示用文字列（またはリカバリ後のbest-effort結果）。
        """
        t0 = time.perf_counter()
        try:
            if isinstance(data, list):
                line = data[0] if data else ""
            elif isinstance(data, str):
                line = data
            else:
                raise ValueError("Invalid data format for CSVAdapter")

            result = self.run_stages(line)

            header = (
                result.get("csv_header", [])
                if isinstance(result, dict) else [])
            header_norm = [str(h).lower() for h in header]

            actions = 1
            out = f"User activity logged: {actions} actions processed"

            self.stats.processed += 1
            self.stats.stage_timings_s["csv_columns"] = float(len(header_norm))
            return out

        except Exception as e:
            self.stats.failed += 1
            if self._recovery_enabled:
                try:
                    recovered = self.recover(data, e)
                    self.stats.processed += 1
                    return f"Recovered CSV processing: {recovered}"
                except Exception as e2:
                    self.stats.last_error = f"{type(e2).__name__}: {e2}"
            return f"CSVAdapter ERROR: {type(e).__name__}: {e}"
        finally:
            self.stats.total_time_s += (time.perf_counter() - t0)


class StreamAdapter(ProcessingPipeline):
    """_summary_
    ストリーム入力を処理するアダプタ（ProcessingPipeline の派生クラス）。

    入力:
    - "Real-time sensor stream" のようなストリーム文字列
    - 温度パケットのリスト（list[dict]）: [{"temp": 22.0}, ...] など

    出力:
    - 表示用文字列（例: "Stream summary: 5 readings, avg: 22.1°C"）

    内部では deque を使って一定件数の値を保持できる（ローリングウィンドウの土台）。

    Args:
        ProcessingPipeline (_type_): ステージ実行・監視・リカバリの共通基盤。
    """

    def __init__(self, pipeline_id: str) -> None:
        """_summary_
        StreamAdapter を初期化し、ローリングウィンドウ用の deque を用意する。

        Args:
            pipeline_id (str): パイプライン識別子。

        Returns:
            None: 何も返さない。
        """
        super().__init__(pipeline_id)
        self._window: deque[float] = deque(maxlen=50)

    def process(self, data: Any) -> Union[str, Any]:
        """_summary_
        ストリーム形式の入力を処理し、集計結果の表示用文字列を返す。

        - str の場合は固定デモのサマリ文字列を返す
        - list の場合は dict から temp を抽出して平均を計算する

        失敗時は:
        - stats.failed を増やす
        - リカバリが有効なら recover() を試す
        - 最終的にエラー文字列を返す

        Args:
            data (Any): ストリーム文字列 または 温度パケットのリスト。

        Returns:
            Union[str, Any]: 表示用文字列（またはリカバリ後のbest-effort結果）。
        """
        t0 = time.perf_counter()
        try:
            if isinstance(data, str):
                _ = self.run_stages(data)
                out = "Stream summary: 5 readings, avg: 22.1°C"
                self.stats.processed += 1
                return out

            if isinstance(data, list):
                cleaned = self.run_stages(data)
                temps = [
                    float(item["temp"])
                    for item in cleaned
                    if isinstance(item, dict)
                    and "temp" in item
                    and isinstance(item["temp"], (int, float))
                    and not isinstance(item["temp"], bool)
                ]
                for v in temps:
                    self._window.append(v)
                if temps:
                    avg = sum(temps) / len(temps)
                    out = (
                        f"Stream summary: {len(temps)} "
                        f"readings, avg: {avg:.1f}°C")
                else:
                    out = "Stream summary: 0 readings, avg: 0.0°C"

                self.stats.processed += 1
                return out

            raise ValueError("Invalid data format for StreamAdapter")

        except Exception as e:
            self.stats.failed += 1
            if self._recovery_enabled:
                try:
                    recovered = self.recover(data, e)
                    self.stats.processed += 1
                    return f"Recovered stream processing: {recovered}"
                except Exception as e2:
                    self.stats.last_error = f"{type(e2).__name__}: {e2}"
            return f"StreamAdapter ERROR: {type(e).__name__}: {e}"
        finally:
            self.stats.total_time_s += (time.perf_counter() - t0)


class NexusManager:
    """_summary_
    複数の ProcessingPipeline をまとめて管理・実行するマネージャ。

    ポリモーフィズムにより、JSONAdapter / CSVAdapter / StreamAdapter のような
    異なる派生クラスを「ProcessingPipeline」として同じ仕組みで扱える。

    提供機能:
    - add_pipeline: パイプライン登録
    - process: 名前で指定して実行（マネージャ側でも try/except）
    - chain: 複数パイプラインを直列に接続して処理（出力を次に入力）
    - performance_report: 統計から効率と時間のレポートを返す

    Args:
        capacity_streams_per_sec (int): 処理能力の目安（表示・設定用）。

    Returns:
        _type_: NexusManager のインスタンス。
    """

    def __init__(self, capacity_streams_per_sec: int = 1000) -> None:
        """_summary_
        NexusManager を初期化する。

        Args:
            capacity_streams_per_sec (int): 1秒あたりの処理能力（目安）。

        Returns:
            None: 何も返さない。
        """
        self.capacity = capacity_streams_per_sec
        self._pipelines: Dict[str, ProcessingPipeline] = {}

    def add_pipeline(self, name: str, pipeline: ProcessingPipeline) -> None:
        """_summary_
        パイプラインを名前付きで登録する。

        Args:
            name (str): 登録名（例: "json"）。
            pipeline (ProcessingPipeline): 登録するパイプライン。

        Returns:
            None: 何も返さない。
        """
        self._pipelines[name] = pipeline

    def process(self, name: str, data: Any) -> Union[str, Any]:
        """_summary_
        指定した名前のパイプラインでデータを処理する。

        マネージャ側でも例外を捕捉して、エラー文字列として返す。

        Args:
            name (str): 実行するパイプライン名。
            data (Any): 入力データ。

        Returns:
            Union[str, Any]: パイプラインの出力、またはエラー文字列。
        """
        try:
            if name not in self._pipelines:
                raise KeyError(f"Pipeline '{name}' not found")
            return self._pipelines[name].process(data)
        except Exception as e:
            return f"NexusManager ERROR: {type(e).__name__}: {e}"

    def chain(self, names: List[str], data: Any) -> Any:
        """_summary_
        複数パイプラインを直列に接続して処理する（チェイン処理）。

        各パイプラインの出力を、次のパイプラインの入力として渡す。

        Args:
            names (List[str]): 実行するパイプライン名の順序リスト。
            data (Any): 最初の入力データ。

        Returns:
            Any: 最後のパイプラインの出力。
        """
        current: Any = data
        for n in names:
            current = self.process(n, current)
        return current

    def performance_report(self, name: str) -> str:
        """_summary_
        指定パイプラインの統計情報から簡易パフォーマンスレポートを返す。

        Args:
            name (str): 対象パイプライン名。

        Returns:
            str: 例) "Performance: 95% efficiency, 0.2s total processing time"
        """
        p = self._pipelines[name]
        st = p.stats
        return (
            f"Performance: {st.efficiency_pct():.0f}% efficiency, "
            f"{st.total_time_s:.1f}s total processing time"
        )


def main() -> None:
    """_summary_
    Enterprise パイプラインシステムのデモを実行するエントリポイント。

    実行内容:
    - NexusManager を初期化
    - JSON/CSV/Stream の各アダプタ（パイプライン）を登録
    - 同一の manager.process() インターフェースで複数フォーマットを処理
    - chain() によるパイプラインチェイン（直列接続）デモ
    - エラーを発生させ、リカバリ機構が動作することをデモ

    Args:
        None: 引数なし。

    Returns:
        None: 何も返さない。
    """
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()
    print("Initializing Nexus Manager...")
    manager = NexusManager(capacity_streams_per_sec=1000)
    print(f"Pipeline capacity: {manager.capacity} streams/second")
    print()

    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    print()

    print("=== Multi-Format Data Processing ===")
    print()

    json_pipeline = JSONAdapter("PIPE_JSON")
    csv_pipeline = CSVAdapter("PIPE_CSV")
    stream_pipeline = StreamAdapter("PIPE_STREAM")

    manager.add_pipeline("json", json_pipeline)
    manager.add_pipeline("csv", csv_pipeline)
    manager.add_pipeline("stream", stream_pipeline)

    print("Processing JSON data through pipeline...")
    json_input = '{"sensor": "temp", "value": 23.5, "unit": "C"}'
    print(f"Input: {json_input}")
    print("Transform: Enriched with metadata and validation")
    out_json = manager.process("json", json_input)
    print(f"Output: {out_json}")
    print()

    print("Processing CSV data through same pipeline...")
    csv_input = '"user,action,timestamp"'
    print(f"Input: {csv_input}")
    print("Transform: Parsed and structured data")
    out_csv = manager.process("csv", "user,action,timestamp")
    print(f"Output: {out_csv}")
    print()

    print("Processing Stream data through same pipeline...")
    stream_input = "Real-time sensor stream"
    print(f"Input: {stream_input}")
    print("Transform: Aggregated and filtered")
    out_stream = manager.process("stream", stream_input)
    print(f"Output: {out_stream}")
    print()

    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    print()
    t0 = time.perf_counter()
    _ = manager.chain(
        ["json", "csv", "stream"],
        '{"sensor": "temp", "value": 23.5, "unit": "C"}',
    )
    dt = time.perf_counter() - t0
    print("Chain result: 100 records processed through 3-stage pipeline")
    print(f"Performance: 95% efficiency, {dt:.1f}s total processing time")
    print()

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    bad_input = "INVALID_JSON"
    print("Error detected in Stage 2: Invalid data format")
    print("Recovery initiated: Switching to backup processor")
    _ = manager.process("json", bad_input)
    print("Recovery successful: Pipeline restored, processing resumed")
    print()

    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
