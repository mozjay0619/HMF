
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




