
HMF
===

HMF (Hierarchical Memmap Format) is a Python package that provides user API similar to that of `PyTables <https://www.pytables.org/>`_ but uses `Numpy memmap <https://numpy.org/doc/stable/reference/generated/numpy.memmap.html>`_  for data storage. It also supports easy data sourcing from Pandas dataframe, as well as parallel writing for fast write speed. 

Install
-------

::

	pip install hierarchical-memmap-format


Getting started
---------------

First, we need to import the packages:

.. code:: python

	import HMF

In order to start working with the HMF, we must invoke ``open_file`` method, which will either create a new directory or read from an existing one. We determine this using ``mode`` argument. We must provide the method with the desired path to the root directory, via ``root_path`` argument, where all data will be written:

.. code:: python

	f = HMF.open_file('myRoot', mode='w+')

Currently, the supported modes are ``w+`` and ``r+``. ``w+`` opens a directory for write. It creates a new directory if it does not exist, and if it exists, it erases the contents of that directory. ``r+`` is for reading and writing, and will read the existing directory contents if it already exists.

Once you are done writing data (reading and writing process is described below), it is *very* important that you invoke ``close`` method to save all the data on disk:

.. code:: python

	f.close()

Writing groups and arrays
-------------------------

With a single "file" handler, the user can easily write and read data using hierarchical file system path. An example will make this clear:

.. code:: python

	f.set_group('/groupA')  # the path must start with root "/"

This code will create a "node", groupA, in which we can write arrays or further groups. The user can create nested directory at once as well:

.. code:: python

	f.set_group('/group1/groupA/groupZ')  

We can write data array using ``set_array`` method:

.. code:: python
	
    array = np.arange(9).reshape(3, 3)

    # array([[0, 1, 2],
    #        [3, 4, 5],
    #        [6, 7, 8]])

    f.set_array('/groupA/array1', array)  

You need not create the group ahead of time. If groupA does not exist, the above code will create the groupA as well. Also, most importatly, the above code will create a memory-map to the array, which you can find out more about `here <https://numpy.org/doc/stable/reference/generated/numpy.memmap.html>`_

Again, once you are done writing data, don't forget to invoke ``close``!

.. code:: python
	
    f.close() 

Reading groups and arrays
-------------------------

You can retrieve both the groups as well as arrays using ``get_group`` and ``get_array`` methods. For example, the below code will retrieve the written array data:

.. code:: python
	
    memmap_obj = f.get_array('/groupA/array1')  

    # memmap([[0, 1, 2],
    #         [3, 4, 5],
    #         [6, 7, 8]])

The returned object is a numpy memmap object that was created earlier. 

You can also use slice or fancy indexing to retrieve partial data using ``idx`` parameter:

.. code:: python

    f.get_array('/groupA/array1', idx=slice(0, 2))

    # memmap([[0, 1, 2],
    #         [3, 4, 5]])

Slicing will return view of the memmap.

.. code:: python

    f.get_array('/groupA/array1', idx=[0, 2])

    # array([[0, 1, 2],
    #        [6, 7, 8]])

Fancy indexing will return copy of the memmap.


Writing node attributes
-----------------------

Here we will demonstrate the self-documenting property of HMF. This again should be no suprise for those familiar with HDF5. HMF allows user to give attribute to each node, whether that is a group node or an array node. Let's try to give some attributes to the groupA node from above. 

.. code:: python
	
	f.set_node_attr('/groupA', key='someAttribute', value='attributeValue')  

Both the key and value of the attribute can be arbitrary Python object. 

You can then retrieve the attributes using ``get_node_attr`` method:

.. code:: python
	
	f.get_node_attr('/groupA', key='someAttribute')

Thus, HMF allows user to write data that is self describing by enabling user to easily read and write accompanying information associated with each node. 

Using with Pandas 
-----------------

Lastly, HMF has API to easily extract array memmap from Pandas dataframes. Also, this mode of writing will be executed in parallel, i.e. all writable arrays will be written in parallel. Let's look at an example, starting from beginning. 

.. code:: python

	import numpy as np
	import pandas as pd

	data = np.arange(10*3).reshape((10, 3))
	pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])

	# 		a	b	c
	#	0	0	1	2
	#	1	3	4	5
	#	2	6	7	8

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

	# Progress: |██████████████████████████████████████████████████| 100.0% Completed!

You can now retrieve the memmap object the usual way:

.. code:: python

	f.get_array('/arrayA')

	# memmap([[1, 2],
	#         [4, 5],
	#         [7, 8]])

Parallel writing 
-----------------

The power of parallel writing shines when you have many arrays to write at once, which would be the case if you have groups of arrays determined by ``groupby`` argument. Let's take another example of dataframe that has groups column:

.. code:: python

    import numpy as np
    import pandas as pd

    data = np.arange(10*3).reshape((10, 3))
    pdf = pd.DataFrame(data=data, columns=['a', 'b', 'c'])

    group_col = ['group_1', 'group_1', 'group_2', 'group_2', 'group_3', 'group_3']
    pdf['groups'] = group_col

    #   	a	b	c	groups
    #   0	0	1	2	group_1
    #   1	3	4	5	group_1
    #   2	6	7	8	group_2
    #   3	9	10	11	group_2
    #   4	12	13	14	group_3
    #   5	15	16	17	group_3

    f = HMF.open_file('pandasExample', mode='w+')

You can then specify ``groupby``:

.. code:: python

    f.from_pandas(pdf, groupby='groups')  # You can also specify "orderby" in order to sort the array by a particular column:
    
    f.register_array('arrayA', ['b', 'c'])
    f.register_array('arrayB', ['a', 'b'])

    f.close()

    # Progress: |██████████████████████████████████████████████████| 100.0% Completed!

Now, when you get the array, the groups have been automatically created, defined by the value of the groupby column, and you can query them using ``get_array``:

.. code:: python

    f.get_array('/group_1/arrayA')  # get data array "arrayA" for partition group "group_1"

    # memmap([[1, 2],
    #         [4, 5]])

    f.get_array('/group_3/arrayB')  # get data array "arrayB" for partition group "group_3"

    # memmap([[12, 13],
    #         [15, 16]])

Getting back dataframe 
-----------------------

What if you want to get the dataframe back instead of numpy array or memmap? You must register dataframe instead of array in this case:

.. code:: python

    f.register_dataframe('arrayA', ['b', 'c'])
    f.register_dataframe('arrayB', ['a', 'b'])

    f.close()

But just as with array, *make sure* that the data type of specified column names is numeric (not even boolean is allowed, convert boolean to 0/1)

Then you can retrieve the data either as numpy array (or memmap) or dataframe: (in both cases, the ``idx`` parameter works the same way)

.. code:: python

    f.get_dataframe('/group_3/arrayB')

    # 		a	b
    #   0	12	13
    #   1	15	16

    f.get_array('/group_3/arrayB')

    # memmap([[12, 13],
    #         [15, 16]])

Convenient methods when working with Pandas
--------------------------------------------

The HMF object, namely ``f`` in our examples, is meant to be used as a single file handler that can be used alone to write and query data easily. The following methods are provided to further this goal of ease of use when ``from_pandas`` is used:

.. code:: python

    f.has_groups()  # returns boolean flag for presence of groups (True if groupby is not None)

    f.get_group_names()  # returns names of the groups 

    f.get_group_sizes()  # returns the sizes of the groups (i.e. number of rows in each group)

    f.get_group_items()  # returns the dict of {name: size}

    f.get_sorted_group_names()  # returns names of the groups sorted by the group size

    f.get_sorted_group_sizes()  # returns sizes of the groups sorted by the group size

    f.get_sorted_group_items()  # returns a list of tuple of (name, size) sorted by the group size


