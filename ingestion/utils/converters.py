from typing import Tuple, Any


def tuple_to_str(t: Tuple[Any, ...], sep: str = " ") -> str:
    return sep.join(map(str, t))
