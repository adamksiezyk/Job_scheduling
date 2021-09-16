import copy
import functools
import time
from typing import TypeVar, Callable, Any

T = TypeVar('T')
R = TypeVar('R')


def sum_dict_val(d: dict[R, T]) -> T:
    """
    Returns the sum of the dict values
    @param d: dictionary
    @return: sum of the values of the dictionary
    """
    return sum(v for v in d.items())


def delete_form_dict(d: dict[R, T], key: R) -> dict[R, T]:
    """
    Returns a dictionary without the (key, value) pair indicated by the key
    @param d: dictionary
    @param key: key
    @return: dictionary without the (key, value) pair
    """
    d_tmp = copy.deepcopy(d)
    del d_tmp[key]
    return d_tmp


def slice_dict(d: dict[R, T], key: R) -> tuple[dict[R, T], dict[R, T]]:
    """
    Returns two dictionaries. The first contains all (key, value) pairs up to the given key.
    The latter contains pairs starting form the key
    @param d: dictionary
    @param key: key to slice
    @return: tuple containing two dictionaries made from the sliced dictionary
    """
    idx = list(d.keys()).index(key)
    items = list(d.items())
    return dict(items[:idx]), dict(items[idx:])


def insert_to_list(l: list[T], idx: int, val: T) -> list[T]:
    """
    Returns the list with the value inserted on the given index
    @param l: list
    @param idx: index
    @param val: value
    @return:
    """
    list_new = copy.deepcopy(l)
    list_new.insert(idx, val)
    return list_new


def append_to_list(l: list[T], val: T) -> list[T]:
    """
    Returns the list with the appended value
    @param l:
    @param val:
    @return:
    """
    return l + [val]


def merge_dicts(d1: dict[R, T], d2: dict[R, T]) -> dict[R, T]:
    """
    Returns a dictionary containing data from both dictionaries
    @param d1: first dictionary
    @param d2: second dictionary
    @return: merged dictionary
    """
    tmp = copy.deepcopy(d1)
    tmp.update(d2)
    return tmp


def flatten(s: list) -> list:
    """
    Recursively flatten a list
    @param s: list
    @return: flattened list
    """
    if not s:
        return s
    if isinstance(s[0], list):
        return flatten(s[0]) + flatten(s[1:])
    return s[:1] + flatten(s[1:])


def get_column(col: int) -> Callable[[list[list[T]]], list[T]]:
    """
    Returns a callable that returns the column from a given matrix
    @param col: column
    @return: callable
    """

    def inner(matrix: list[list[T]]) -> list[T]:
        """
        Returns the column from the given matrix
        @param matrix: matrix
        @return: column
        """
        return functools.reduce(lambda ans, row: append_to_list(ans, row[col]), matrix, [])

    return inner


def get_value_form_all(key: R) -> Callable[[list[dict[R, T]]], list[T]]:
    """
    Returns a callable that returns the value indicated by the key form all dictionaries
    @param key: key
    @return: callable
    """

    def inner(matrix: list[dict[R, T]]) -> list[T]:
        """
        Returns the value indicated by the key from all dicts from the matrix
        @param matrix: matrix
        @return: list of values
        """
        return functools.reduce(lambda ans, row: append_to_list(ans, row[key]), matrix, [])

    return inner


def pipeline(data: T, functions: list[Callable]) -> Any:
    """
    Executes each function on the given data
    @param data: data
    @param functions: list of functions
    @return: processed data
    """
    return functools.reduce(lambda ans, f: f(ans), functions, data)


def call(function: Callable[[R, ...], T], argument: R) -> Callable[[...], T]:
    """
    Initializes the function with the given argument
    @param function:
    @param argument:
    @return:
    """

    def apply_function(*args, **kwargs):
        """
        Executes the function
        @param args: arguments
        @param kwargs: keyword arguments
        @return: T
        """
        return function(argument, *args, **kwargs)

    return apply_function


def timer(func: Callable[[...], T]) -> Callable[[...], T]:
    """
    Decorator to measure function execution time
    @param func: function
    @return:
    """

    def wrapper(*arg, **kw) -> T:
        """
        Executes the function
        @param arg: arguments
        @param kw: keyword arguments
        @return: T
        """
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()

        print(func.__name__, ': ', t2 - t1)
        return res

    return wrapper
