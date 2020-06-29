import pytest

import numpy as np
import pandas as pd
import HMF

def check_equal(ar1, ar2):
    return np.equal(ar1, ar2).sum() == ar1.shape[0] * ar1.shape[1]

def test_writing_arrays():
	"""test writing arrays"""

	f = HMF.open_file('asdf', mode='w+', verbose=False)
	f.set_array('/group1/array1', np.arange(3))
	f.set_array('/group1/array2', np.arange(3))
	f.set_array('/group2/array2', np.arange(3))
	f.set_array('/group2/subgroup1/array2', np.arange(3))
	f.close()




