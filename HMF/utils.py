import numpy as np
import pickle


def save_obj(obj, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
def load_obj(filepath):
    with open(filepath, 'rb') as f:
        return pickle.load(f)

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

