import functools

# Sum dictionary values
def sum_dict_val(_dict, init_val):
    return functools.reduce(lambda sum, key: sum + _dict[key], _dict, init_val)

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
def append(_list, element):
    return _list + [element]

# Flatten a list
def flatten(S):
    if S == []:
        return S
    if isinstance(S[0], list):
        return flatten(S[0]) + flatten(S[1:])
    return S[:1] + flatten(S[1:])

# Get column of matrix
def get_column(col):
    def inner(matrix):
        return functools.reduce(lambda ans, row: append(ans, row[col]), matrix, [])
    return inner

# Run each function on data
def pipeline(data, functions):
    return functools.reduce(lambda ans, f: f(ans), functions, data)

# Init a function with one argument and call it with the second
def call(function, argument):
    def apply_function(record):
        return list(function(argument, record))
    return apply_function
