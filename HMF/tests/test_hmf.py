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

# def test_from_pandas_API():
# 	"""test writing using from_pandas API and also reading"""

# 	data = np.arange(10*3).reshape((10, 3))
# 	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
# 	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
# 	pdf['group'] = groups

# 	f = HMF.open_file('asdf', mode='w+', verbose=False)
# 	f.from_pandas(pdf, groupby='group', orderby='c')
# 	f.register_array('arrayA', ['b', 'c'])
# 	f.register_array('arrayB', ['a', 'b'])
# 	f.close()

# 	f = HMF.open_file('asdf', mode='r+')
# 	a_arrayA = f.get_array('/a/arrayA')
# 	b_arrayB = f.get_array('/b/arrayB')

# 	assert check_equal(pdf[pdf['group']=='b'][['a', 'b']].values, b_arrayB)


