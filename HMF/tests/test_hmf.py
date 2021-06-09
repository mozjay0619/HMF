import pytest
import multiprocessing
print(multiprocessing.get_start_method())

import numpy as np
import pandas as pd
import HMF


def test_writing_arrays():
	"""test writing arrays"""

	f = HMF.open_file('test_file', mode='w+', verbose=False)
	f.set_array('/group1/array1', np.arange(3))
	f.set_array('/group1/array2', np.arange(3))
	f.set_array('/group2/array2', np.arange(3))
	f.set_array('/group2/subgroup1/array2', np.arange(3))
	f.close()

	assert np.array_equal(np.arange(3), f.get_array('/group1/array1'))
	assert np.array_equal(np.arange(3), f.get_array('/group1/array2'))
	assert np.array_equal(np.arange(3), f.get_array('/group2/array2'))
	assert np.array_equal(np.arange(3), f.get_array('/group2/subgroup1/array2'))

def test_writing_and_reading_arrays():
	
	f = HMF.open_file('test_file', mode='w+', verbose=False)
	f.set_array('/group1/array1', np.arange(3))
	f.set_array('/group1/array2', np.arange(3))
	f.set_array('/group2/array2', np.arange(3))
	f.set_array('/group2/subgroup1/array2', np.arange(3))
	f.close()

	f = HMF.open_file('test_file', mode='r+', verbose=False)

	assert np.array_equal(np.arange(3), f.get_array('/group1/array1'))
	assert np.array_equal(np.arange(3), f.get_array('/group1/array2'))
	assert np.array_equal(np.arange(3), f.get_array('/group2/array2'))
	assert np.array_equal(np.arange(3), f.get_array('/group2/subgroup1/array2'))

def test_writing_arrays_from_pandas():

	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)
	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_array('arrayA', ['b', 'c'])
	f.register_array('arrayB', ['a', 'b'])
	f.close()

	assert np.array_equal(f.get_array('/a/arrayA'), pdf[pdf['group']=='a'][['b', 'c']].values)
	assert np.array_equal(f.get_array('/b/arrayB'), pdf[pdf['group']=='b'][['a', 'b']].values)

def test_writing_and_reading_arrays_from_pandas():

	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)
	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_array('arrayA', ['b', 'c'])
	f.register_array('arrayB', ['a', 'b'])
	f.close()

	f = HMF.open_file('test_file', mode='r+', verbose=False)

	assert np.array_equal(f.get_array('/a/arrayA'), pdf[pdf['group']=='a'][['b', 'c']].values)
	assert np.array_equal(f.get_array('/b/arrayB'), pdf[pdf['group']=='b'][['a', 'b']].values)

def test_writing_dataframes_from_pandas():

	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)
	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])
	f.close()

	assert np.array_equal(f.get_array('/a/dataframeA'), pdf[pdf['group']=='a'][['b', 'c']].values)
	assert np.array_equal(f.get_array('/b/dataframeB'), pdf[pdf['group']=='b'][['a', 'b']].values)

def test_writing_and_reading_dataframes_from_pandas():
	
	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)
	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])
	f.close()

	f = HMF.open_file('test_file', mode='r+', verbose=False)

	assert np.array_equal(f.get_array('/a/dataframeA'), pdf[pdf['group']=='a'][['b', 'c']].values)
	assert np.array_equal(f.get_array('/b/dataframeB'), pdf[pdf['group']=='b'][['a', 'b']].values)

def test_multi_pdf_writing():
	
	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)

	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.close()

	np.array_equal(f.get_dataframe('/__specialHMF__dataFrameNumber_0/a/dataframeA'), 
               pdf[pdf['group']=='a'][['b', 'c']].values)
	np.array_equal(f.get_dataframe('/__specialHMF__dataFrameNumber_0/b/dataframeB'), 
               pdf[pdf['group']=='b'][['a', 'b']].values)

def test_multi_pdf_writing_reading():
	
	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)

	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.from_pandas(pdf, groupby='group', orderby='c')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.close()

	f = HMF.open_file('test_file', mode='r+', verbose=False)

	np.array_equal(f.get_dataframe('/__specialHMF__dataFrameNumber_0/a/dataframeA'), 
               pdf[pdf['group']=='a'][['b', 'c']].values)
	np.array_equal(f.get_dataframe('/__specialHMF__dataFrameNumber_0/b/dataframeB'), 
               pdf[pdf['group']=='b'][['a', 'b']].values)

def test_multi_pdf_writing_using_group_names():

	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)

	f.from_pandas(pdf, groupby='group', orderby='c', group_name='groupA')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.from_pandas(pdf, groupby='group', orderby='c', group_name='groupB')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.close()

	np.array_equal(f.get_dataframe('/groupA/a/dataframeA'), 
               pdf[pdf['group']=='a'][['b', 'c']].values)
	np.array_equal(f.get_dataframe('/groupB/b/dataframeB'), 
               pdf[pdf['group']=='b'][['a', 'b']].values)

def test_multi_pdf_writing_and_reading_using_group_names():

	data = np.arange(10*3).reshape((10, 3))
	groups = ['a'] * 3 + ['b'] * 3 + ['c'] * 4
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])
	pdf['group'] = groups

	f = HMF.open_file('test_file', mode='w+', verbose=False)

	f.from_pandas(pdf, groupby='group', orderby='c', group_name='groupA')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.from_pandas(pdf, groupby='group', orderby='c', group_name='groupB')
	f.register_dataframe('dataframeA', ['b', 'c'])
	f.register_dataframe('dataframeB', ['a', 'b'])

	f.close()

	f = HMF.open_file('test_file', mode='r+', verbose=False)

	np.array_equal(f.get_dataframe('/groupA/a/dataframeA'), 
               pdf[pdf['group']=='a'][['b', 'c']].values)
	np.array_equal(f.get_dataframe('/groupB/b/dataframeB'), 
               pdf[pdf['group']=='b'][['a', 'b']].values)

