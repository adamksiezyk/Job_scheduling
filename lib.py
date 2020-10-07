import functools
from datetime import timedelta

# Sum dictionary values
def sum_dict_val(_dict, init_val=timedelta(0)):
    if isinstance(_dict, dict):
        return functools.reduce(lambda sum, key: sum + _dict[key], _dict, init_val)
    elif isinstance(_dict, list):
        return functools.reduce(lambda sum, elem: sum + sum_dict_val(elem), _dict, init_val)
    return init_val

def delelte(_dictionary, key):
    import copy
    dictionary_new = copy.deepcopy(_dictionary)
    del dictionary_new[key]
    return dictionary_new

# Slice directory, return from key
def slice_dict(dictionary, key_slice):
    index = list(dictionary.keys()).index(key_slice)
    items = list(dictionary.items())
    return dict(items[:index]), dict(items[index:])

# Insert into list
def insert(_list, index, value):
    import copy
    list_new = copy.deepcopy(_list)
    list_new.insert(index, value)
    return list_new

# Append to list
def append(first, second):
    import copy
    if isinstance(first, list):
        return first + [second]

def merge(first, second):
    import copy
    if isinstance(first, list) and isinstance(second, list):
        return first + second
    if isinstance(first, dict) and isinstance(second, dict):
        tmp = copy.deepcopy(first)
        tmp.update(second)
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
