import pytest

import numpy as np
import HMF


def test_writing_arrays():
	"""asdf"""

	f = HMF.open_file('asdf', mode='w+', verbose=False)
	f.set_array('/group1/array1', np.arange(3))
	f.set_array('/group1/array2', np.arange(3))
	f.set_array('/group2/array2', np.arange(3))
	f.set_array('/group2/subgroup1/array2', np.arange(3))
	f.close()




