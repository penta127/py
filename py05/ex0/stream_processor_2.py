#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        # list[int|float] を想定
        if isinstance(data, list) is False:
            return False
        if len(data) == 0:
            return False
        for x in data:
            if isinstance(x, (int, float)) is False:
                return False
        return True

    def process(self, data: Any) -> str:
        if self.validate(data) is False:
            raise ValueError(f"Invalid numeric data: {data!r}")

        numbers: list[float] = [float(x) for x in data]
        total = sum(numbers)
        count = len(numbers)
        avg = total / count
        return f"Processed {count} numeric values, sum={int(total) if total.is_integer() else total}, avg={avg}"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str) is False:
            return False
        if data.strip() == "":
            return False
        return True

    def process(self, data: Any) -> str:
        if self.validate(data) is False:
            raise ValueError(f"Invalid text data: {data!r}")

        text: str = data
        chars = len(text)
        words = len(text.split())
        return f"Processed text: {chars} characters, {words} words"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str) is False:
            return False
        if ":" not in data:
            return False
        return True

    def process(self, data: Any) -> str:
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
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    runs = [
        ("Numeric", NumericProcessor(), [1, 2, 3, 4, 5], "Numeric data verified"),
        ("Text", TextProcessor(), "Hello Nexus World", "Text data verified"),
        ("Log", LogProcessor(), "ERROR: Connection timeout", "Log entry verified"),
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
