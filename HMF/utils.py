import numpy as np
import pickle
import functools
import time

def failed_method_retry(method, max_retries=3):
    """This is a decorator for allowing certain methods to retry itself in cases of
    failures, as many as max_retries times. This decorator can work together with 
    the above two artificial traceback utility methods. 

    Example usage:

        @object_exception_catcher
        @failed_method_retry
        def run_transformer(self, df, hist_df):

        This will first allow the method to be retried, and then after max_retries times
        of retries, the exception catcher decorator will catch the final exception.
    """
    
    @functools.wraps(method)
    def failed_method_retried(*args, **kwargs):
        
        error = None
        
        for i in range(max_retries):
            
            try:
                return method(*args, **kwargs)
            
            except Exception as e:
                error = e
                print('[{}] method failed due to {}'.format(method.__name__, error))

                time.sleep(0.1)
                continue
            
        else:
            raise error
    
    return failed_method_retried


def save_obj(obj, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
@failed_method_retry
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

