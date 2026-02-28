# a decorator that times any function
from functools import wraps
from typing import Callable, Any
import time

def timer(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Function {func.__name__} took {end - start} seconds to execute")
        return result
    return wrapper


def expensive_function(x: int) -> int:
    time.sleep(2)
    return x * x


def main():
    print(expensive_function(4))


if __name__ == "__main__":
    main()
