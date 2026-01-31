#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any

<<<<<<< HEAD
from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass
=======
class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass
    
    @abstractmethod
    def process(self, data: Any) -> str:
        pass
>>>>>>> 0571e26 (py5)

    def format_output(self, result: str) -> str:
        return f"Output: {result}"

<<<<<<< HEAD
class NumericProcessor(DataProcessor):


def stream_processor():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

if __name__ == "__main__":
    stream_processor()
=======

class NumericProcessor(DataProcessor):
    """dataに型が渡されて、int floatなら数値とみなす
    """
    def validate(self, data: Any) -> bool:
        re_int = isinstance(data, int)
        re_float = isinstance(data, float)

        if re_int is True:
            return True
        if re_float is True:
            return True
        return False

    def process(self, data: Any) -> str:
        if self.validate(data) is False:
            raise ValueError(f"only int or float. not {data} ")
        number = data
        doubled = number * 2
        return f"{number} doubled is {doubled}"
        

class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        re_str = isinstance(data, str)
        
        if re_str is False:
            return False
        if data.strip() == "":
            return False
        return True
    
    def process(self, data: Any) -> str:
        if self.validate(data) is False:
            raise ValueError(f"only non-empty str, not {data!r}")

        text = data
        words = text.split()
        count = len(words)
        return f"word_count={count} text='{text}'"

class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        re_str = isinstance(data, str)
        
        if re_str is False:
            return False
        if ":" not in data:
            return False
        return True
    
    def process(self, data: Any) -> str:
        if self.validate(data) is False:
            raise ValueError(f"only log-like str 'LEVEL: msg', not {data!r}")

        level, message = data.split(":", 1)
        message = message.strip()
        level = level.strip().upper()
        return f"level={level} message={message}"



def stream_processor() -> None:
    print("=== CODE NEXUS - DATA RECOVERY SYSTEM ===")
    print()
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    data_list = [
        ("Numeric", NumericProcessor(), [1, 2, 3, 4, 5]),
        ("Text", TextProcessor(), ["Hello Nexus World"]),
        ("Log", LogProcessor(), ["ERROR: Connection timeout"]),
    ]

    for name, processor, items in data_list:
        print(f"[{name}]")
        for item in items:
            try:
                if processor.validate(item) is False:
                    print(f"  SKIP: {item!r}")
                    continue

                result = processor.process(item)
                print(f"  {processor.format_output(result)}")
            except (ValueError, TypeError) as e:
                print(f"  ERROR: {e}")
        print()


if __name__ == "__main__":
    stream_processor()
>>>>>>> 0571e26 (py5)
