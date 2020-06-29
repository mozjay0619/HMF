
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

Currently, the supported modes are ``w+`` and ``r+``. ``w+`` opens a directory for read as well as write. It creates a new directory if it does not exist, and if it exists, it erases the contents of that directory. ``r+`` is for reading and writing, and will read the existing directory contents if it already exists.




