import numpy as np
import pickle
import functools
import time
import os

def save_obj(obj, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
def load_obj(filepath):
    # with open(filepath, 'rb') as f:
    #     return pickle.load(f)


    f = os.open(filepath, os.O_RDONLY|os.O_NONBLOCK)
    
    readable = os.fdopen(f, "rb", 0)

    if select.select([readable], [], [], timeout=10)[0][0] == readable:

        return pickle.load(readable)

def stride_util(array, window_size, skip_size, dtype):
    
    return(np.lib.stride_tricks.as_strided(
        array, 
        shape=[np.ceil(len(array)-1).astype(dtype), window_size],
        strides=[array.strides[0]*skip_size, array.strides[0]]))

def border_idx_util(array):
    
    border_idx = np.where(np.diff(array) != 0)[0] + 1
    border_idx = border_idx.astype(np.int)

    border_idx = np.insert(border_idx, 0, 0, axis=0)
    border_idx = np.append(border_idx, [len(array)], axis=0)
    
    return border_idx

def write_memmap(filepath, dtype, shape, array):

    writable_memmap = np.memmap(filepath, dtype=dtype, mode="w+", shape=shape)
    writable_memmap[:] = array[:]
    del writable_memmap

def read_memmap(filepath, dtype, shape, idx=None):

    readonly_memmap = np.memmap(filepath, dtype=dtype, mode="r", shape=shape)

    if idx is None:
        array = readonly_memmap[:]
    else:
        if is_nested_list(idx):
            array = readonly_memmap[tuple(idx)]
        else:
            array = readonly_memmap[idx]

    del readonly_memmap
    return array

def is_nested_list(l):
    
    if not isinstance(l, list):
        return False
    else:
        return any(isinstance(i, list) for i in l)

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print(f'\r', end = printEnd)

