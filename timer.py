# a decorator that times any function
from functools import wraps
from typing import Callable, Any
import time


def get_time(func: Callable) -> Callable:
    """ gets time of function execution"""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """ wrapper function that times the function execution"""
        start_time: float = time.perf_counter()
        result = func(*args, **kwargs)
        end_time: float = time.perf_counter()
        print(f"Function {func.__name__} took {end_time - start_time:.2f} seconds to execute")
        return result

    return wrapper

@get_time
def expensive_function(x: int) -> int:
    """ my expensive function that takes a long time to execute"""
    time.sleep(2)
    return x * x


def main():
    print(expensive_function.__name__)
    print(expensive_function.__doc__)
    print(expensive_function.__annotations__)
    print(expensive_function(4))


if __name__ == "__main__":
    main()
