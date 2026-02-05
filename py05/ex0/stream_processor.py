#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """
    データ処理の共通インターフェースを定義する抽象基底クラス。

    Args:
        ABC (_type_):抽象基底クラスを実現するための親クラス（ABC）。

    Returns:
        _type_:このクラス自体はインスタンス化して使うものではなく、
        サブクラスが共通のメソッド形（validate/process/format_output）を持つための土台。
    """
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """
        入力データがこのプロセッサで処理可能かどうかを検証する。

        Args:
            data (Any): 検証対象の入力データ。

        Returns:
            bool: 処理可能なら True、不可能なら False。
        """
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        """
        入力データを処理して結果文字列を返す。

        Args:
            data (Any):処理対象の入力データ。

        Returns:
            str: 処理結果を表す文字列。
        """
        pass

    def format_output(self, result: str) -> str:
        """
        処理結果の文字列を表示用に整形する（デフォルト実装）。

        Args:
            result (str): process() が返した処理結果文字列。

        Returns:
            str: 表示用の整形済み文字列。
        """
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """
    数値リスト（int/float のリスト）を処理するプロセッサ。

    Args:
        DataProcessor (_type_):DataProcessor (_type_): 数値処理用の
          validate/process を提供するために継承する親クラス。
    """
    def validate(self, data: Any) -> bool:
        """
        入力が「空でない数値リスト」かどうかを検証する。

        想定する入力:
            - list 型
            - 空ではない
            - 全要素が int または float

        Args:
            data (Any):検証対象の入力データ。

        Returns:
            bool: 条件を満たせば True、満たさなければ False。
        """
        if isinstance(data, list) is False:
            return False
        if len(data) == 0:
            return False
        for x in data:
            if isinstance(x, (int, float)) is False:
                return False
        return True

    def process(self, data: Any) -> str:
        """
        数値リストを処理し、件数・合計・平均を文字列で返す。

        Args:
            data (Any): 数値リスト（list[int|float]）を想定。

        Raises:
            ValueError: data が数値リストとして不正な場合に送出する。

        Returns:
            str:  例) "Processed 5 numeric values, sum=15, avg=3.0"
        """
        if self.validate(data) is False:
            raise ValueError(f"Invalid numeric data: {data!r}")

        numbers: list[float] = [float(x) for x in data]
        total = sum(numbers)
        count = len(numbers)
        avg = total / count
        return (
            f"Processed {count} numeric values, sum="
            f"{int(total) if total.is_integer() else total}, avg={avg}")


class TextProcessor(DataProcessor):
    """
    テキスト（空白だけではない文字列）を処理するプロセッサ。

    Args:
        DataProcessor (_type_): テキスト処理用の validate/process を提供するために継承する親クラス。
    """
    def validate(self, data: Any) -> bool:
        """
        入力が「空白だけではない文字列」かどうかを検証する。

        Args:
            data (Any): 検証対象の入力データ。

        Returns:
            bool: 条件を満たせば True、満たさなければ False。
        """
        if isinstance(data, str) is False:
            return False
        if data.strip() == "":
            return False
        return True

    def process(self, data: Any) -> str:
        """テキストを処理し、文字数と単語数を文字列で返す。

        Args:
            data (Any): 文字列を想定。

        Raises:
            ValueError: data が文字列として不正な場合に送出する。

        Returns:
            str: 例) "Processed text: 17 characters, 3 words"
        """
        if self.validate(data) is False:
            raise ValueError(f"Invalid text data: {data!r}")

        text: str = data
        chars = len(text)
        words = len(text.split())
        return f"Processed text: {chars} characters, {words} words"


class LogProcessor(DataProcessor):
    """
    ログ文字列（'LEVEL: message' 形式）を処理するプロセッサ。

    Args:
        DataProcessor (_type_): ログ処理用の validate/process を提供するために継承する親クラス。
    """
    def validate(self, data: Any) -> bool:
        """入力がログ形式（':' を含む文字列）かどうかを検証する。

        Args:
            data (Any): 検証対象の入力データ。

        Returns:
            bool: 条件を満たせば True、満たさなければ False。
        """
        if isinstance(data, str) is False:
            return False
        if ":" not in data:
            return False
        return True

    def process(self, data: Any) -> str:
        """
        ログを解析してレベルに応じたタグ付きメッセージを返す。

        仕様（この課題の例に合わせた挙動）:
            - level が "ERROR" のとき tag は "ALERT"
            - それ以外は tag を "INFO" とする

        Args:
            data (Any): "LEVEL: message" 形式の文字列を想定。

        Raises:
            ValueError: data がログ形式として不正な場合に送出する。

        Returns:
            str: 例) "[ALERT] ERROR level detected: Connection timeout"
        """
        if self.validate(data) is False:
            raise ValueError(f"Invalid log entry: {data!r}")

        level, message = data.split(":", 1)
        level = level.strip().upper()
        message = message.strip()

        tag = "INFO"
        if level == "ERROR":
            tag = "ALERT"

        return f"[{tag}] {level} level detected: {message}"


def stream_processor() -> None:
    """
    各プロセッサを使って処理を実行し、ポリモーフィズムをデモする。

    同じ DataProcessor インターフェース（validate/process/format_output）で
    数値・テキスト・ログをそれぞれ処理できることを表示する。
    """
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    runs = [
        (
            "Numeric",
            NumericProcessor(),
            [1, 2, 3, 4, 5],
            "Numeric data verified"
            ),
        (
            "Text",
            TextProcessor(),
            "Hello Nexus World",
            "Text data verified"
            ),
        (
            "Log",
            LogProcessor(),
            "ERROR: Connection timeout",
            "Log entry verified"
            ),
    ]

    for name, processor, data, validation_msg in runs:
        print(f"Initializing {name} Processor...")
        print(f"Processing data: {data!r}")

        if processor.validate(data) is False:
            print("Validation: failed")
            print()
            continue

        print(f"Validation: {validation_msg}")
        result = processor.process(data)
        print(processor.format_output(result))
        print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    demo: list[tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello Nexus"),
        (LogProcessor(), "INFO: System ready"),
    ]

    idx = 1
    for processor, data in demo:
        result = processor.process(data)
        print(f"Result {idx}: {result}")
        idx += 1

    print()
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    stream_processor()
