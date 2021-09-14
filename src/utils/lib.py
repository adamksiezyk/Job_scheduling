import copy
import functools
from typing import TypeVar, Any

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


def merge_dicts(d1, d2):
    tmp = copy.deepcopy(d1)
    tmp.update(d2)
    return tmp


# Flatten a list
def flatten(s):
    if s == []:
        return s
    if isinstance(s[0], list):
        return flatten(s[0]) + flatten(s[1:])
    return s[:1] + flatten(s[1:])


# Get column of matrix
def get_column(col):
    def inner(matrix):
        if isinstance(matrix, list):
            return functools.reduce(lambda ans, row: append(ans, row[col]), matrix, [])
        return functools.reduce(lambda ans, row: merge(ans, {row: matrix[row][col]}), matrix, {})

    return inner


# Run each function on data
def pipeline(data, functions):
    return functools.reduce(lambda ans, f: f(ans), functions, data)


# Init a function with one argument and call it with the second
def call(function, argument):
    def apply_function(record):
        return list(function(argument, record))

    return apply_function


# Measure function execution time
def timer(func):
    def wrapper(*arg, **kw):
        import time
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()

        print(func.__name__, ': ', t2 - t1)
        return res

    return wrapper


def next_weekday(date, weekday):
    # Input:
    #   - date - datetime date
    #   - weekday: 0 = Monday, 1=Tuesday, 2=Wednesday...
    # Output:
    #   - next weekday date
    from datetime import datetime, time, timedelta
    days_ahead = weekday - date.weekday()
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    return datetime.combine(date.date() + timedelta(days_ahead), time(hour=6))


def next_n_days(date, n):
    # Input:
    #   - date - datetime date
    #   - n - days amount
    # Output:
    #   - date + n days morning
    if not n: return date
    from datetime import timedelta, datetime, time
    return datetime.combine(date.date() + timedelta(days=n), time(hour=6))
