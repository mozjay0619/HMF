
HMF
===

HMF (Hierarchical Memmap Format) is a Python package that provides user API similar to that of `PyTables <https://www.pytables.org/>`_ but uses `Numpy memmap <https://numpy.org/doc/stable/reference/generated/numpy.memmap.html>`_  for data storage. It also supports easy data sourcing from Pandas dataframe, as well as parallel writing for fast write speed. 

Install
-------

::

	pip install hierarchical-memmap-format


Getting started
---------------

The HMF APIs are largely inspired by those of PyTables, and hence supports two of the important functionalities of HDF5 in that they allow the user to write data that is self-organizing and self-documenting. We will demonstrate these ideas through an example. 

First, we need to import the package:

.. code:: python

	import HMF

In order to start working with the HMF, we must invoke ``open_file`` method, which will either create a new directory or read from an existing one, which we determine through ``mode`` argument. Note that even though it is called open "file", the word file is loosely used to mean "directory". As such, we must provide the method with the desired path to the root directory, via ``root_path`` argument, where all data will be written:

.. code:: python

	f = HMF.open_file('myRoot', mode='w+')

Currently, the supported modes are ``w+`` and ``r+``. ``w+`` opens a directory for write. It creates a new directory if it does not exist, and if it exists, it erases the contents of that directory. ``r+`` is for reading and writing, and will read the existing directory contents if it already exists.

Once you are done writing data (reading and writing process is described below), it is *very* important that you invoke ``close`` method to save all the data on disk:

.. code:: python

	f.close()

Writing groups and arrays
-------------------------

Here we will demonstrate the self-organizing property of HMF. With a single "file" handler, the user can easily write data using hierarchical file system. This will be easier to understand if you are already familiar with HDF5. 

.. code:: python

	f.set_group('/groupA')  # the path must start with root "/"

This code will create a "directory", or "node", groupA, in which we can write arrays or further groups. The user can create nested directory at once as well:

.. code:: python

	f.set_group('/group1/groupA/groupZ')  

We can write array using ``set_array`` method:

.. code:: python
	
	array = np.arange(9)
	f.set_array('/groupA/array1', array)  

You need not create the group ahead of time. If groupA does not exist, the above code will create the groupA as well. Also, most importatly, the above code will create a memory-map to the array, which you can find out more about `here <https://numpy.org/doc/stable/reference/generated/numpy.memmap.html>`_

You can retrieve both the groups as well as arrays using ``get_group`` and ``get_array`` methods. For example, the below code will retrieve the written array data:

.. code:: python
	
	memmap_obj = f.get_array('/groupA/array1')  

The returned object is a numpy memmap object that was created earlier. Again, once you are done writing data, don't forget to invoke ``close``!

.. code:: python
	
	f.close() 

Writing node attributes
-----------------------

Here we will demonstrate the self-documenting property of HMF. This again should be no suprise for those familiar with HDF5. HMF allows user to give attribute to each node, whether that is a group node or an array node. Let's try to give some attributes to the groupA node from above. 

.. code:: python
	
	f.set_node_attr('/groupA', key='someAttribute', value='attributeValue')  

Both the key and value of the attribute can be arbitrary Python object. 

You can then retrieve the attributes using ``get_node_attr`` method:

.. code:: python
	
	f.set_node_attr('/groupA', key='someAttribute')

Thus, HMF allows user to write data that is self describing by enabling user to easily read and write accompanying information associated with each node. 

Using with Pandas 
-----------------

Lastly, HMF has API to easily extract array memmap from Pandas dataframes. Also, this mode of writing will be executed in parallel, i.e. all writable arrays will be written in parallel. Let's look at an example, starting from beginning. 

.. code:: python

	import numpy as np
	import pandas as pd

	data = np.arange(10*3).reshape((10, 3))
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])

	f = HMF.open_file('pandasExample', mode='w+')

You first introduce the dataframe to HMF like so:

.. code:: python

	f.from_pandas(pdf)

You can then "register" arrays from the dataframe one by one:

.. code:: python

	f.register_array('arrayA', ['b', 'c'])
	f.register_array('arrayB', ['a', 'b'])

Finally calling ``close`` to save the data:

.. code:: python

	f.close()

You can now retrieve the memmap object the usual way:

.. code:: python

	f.get_array('/arrayA')

The power of parallel writing shines when you have many arrays to write at once, which would be the case if you have groups of arrays determined by ``groupby`` argument. Let's take another example of dataframe that has groups column:

.. code:: python

	import numpy as np
	import pandas as pd

	data = np.arange(10*3).reshape((10, 3))
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])

	group_col = ['Aaa', 'Aaa', 'Aaa', 'Bbb', 'Bbb', 'Bbb', 'Ccc', 'Ccc', 'Ccc', 'Ccc']
	pdf['groups'] = group_col

	f = HMF.open_file('pandasExample', mode='w+')

You can then specify ``groupby``:

.. code:: python

	f.from_pandas(pdf, groupby='groups')  # You can also specify "orderby" in order to sort the array by a particular column!
	
	f.register_array('arrayA', ['b', 'c'])
	f.register_array('arrayB', ['a', 'b'])

	f.close()

Now, when you get the array, the groups have been automatically created, defined by the value of the groupby column:

.. code:: python

	f.get_array('/Aaa/arrayA')  # get arrayA for partition group "Aaa"
	f.get_array('/Ccc/arrayB')  # get arrayB for partition group "Ccc"













