from .core import BaseHMF
from .utils import stride_util
from .utils import border_idx_util
from .utils import save_obj
from .utils import load_obj
from .parallel import WriterProcessManager

import numpy as np
import pandas as pd
import os
import shutil
import psutil
from multiprocessing import sharedctypes
from collections import defaultdict

from . import constants

import warnings
def format_Warning(message, category, filename, lineno, line=''):
    return str(filename) + ':' + str(lineno) + ': ' + category.__name__ + ': ' +str(message) + '\n'

class WRITE_TASK_FAILED(UserWarning):
    pass

class READ_TASK_FAILED(UserWarning):
    pass


def open_file(root_path, mode='w+', verbose=False):
    """
    Available modes: 

        w: write only. Truncate if exists.
        w+: read and write. Truncate if exists.
        r: read only.
        r+: read and write. Do not truncate if exists.
    """

    if(mode=='w+'):

        # check if path exists
        # check if it is conforming
        # 
        if(os.path.exists(root_path)):

            shutil.rmtree(root_path)
        
        os.mkdir(root_path)

        from_existing = False
        memmap_map = None

    elif(mode=='r+'):

        # if(os.path.exists(root_path)):

        is_hmf_directory_flag, memmap_map = is_hmf_directory(root_path)

            # if(is_hmf_directory_flag):

            #     from_existing = True

            # else:

            #     from_existing = False

            #     # warn that it is not
            #     # memmap not there
            #     # try to recover...

            #     pass

    hmf = HMF(root_path, memmap_map, verbose)

    return hmf

def is_hmf_directory(root_path):
    """Assuming root_path exists:
    1. check memmap_map exists
    2. check files are all present 

    Needs much improvements...
    """

    # file_list = os.listdir(root_path)

    # if MEMMAP_MAP_FILENAME not in file_list:
    #     print('memmap_map not present')
    #     return(False, None)

    if not fail_safe_check_obj(root_path, constants.MEMMAP_MAP_FILENAME):

        print('memmap_map not present')
        return(False, None)



    memmap_map = fail_safe_load_obj(os.path.join(root_path, constants.MEMMAP_MAP_FILENAME))

    # array_file_list = get_all_array_dirpaths(memmap_map)

    # if(not set(array_file_list) < set(file_list)):

    #     return(False, None)

    return(True, memmap_map)

def get_all_array_dirpaths(m):
    
    visited_dirpath = []
    array_dirpaths = []
    
    _depth_first_search(m, 'HMF_rootNodeKey', visited_dirpath, array_dirpaths)
    
    return array_dirpaths

def _depth_first_search(m, k, visited_dirpaths, array_dirpaths):
    """
    DFS(G, k):
        mark k as visited
        for node l pointed to by node k:
            if node l is not visited:
                DFS(G, l)
                
    * we are marking visits by dirpath not by node name
    * we need a separate list recording array dirpaths
    * when we reach array node, we must stop the search
      because that node does not have [ nodes ] key.
      (or could make an empty nodes dict...)
    
    Parameters
    ----------
    m : dict
        where we can query by k (the node name level map)
        also, the map needs to have been positioned by level
        
    """
    
    if(k == 'HMF_rootNodeKey'):
        cur_dirpath = m['dirpath']
        visited_dirpaths.append(cur_dirpath)
        node_pos = m['nodes']
    
    else:
        cur_dirpath = m[k]['dirpath']
        visited_dirpaths.append(cur_dirpath)
        if(m[k]['node_type']=='array'):
            array_dirpaths.append(cur_dirpath)
            return
        
        node_pos = m[k]['nodes']
    
    for l, v in node_pos.items():
        
        cur_dirpath = node_pos[l]['dirpath']
        
        if(not cur_dirpath in visited_dirpaths):
            _depth_first_search(node_pos, l, visited_dirpaths, array_dirpaths)
    

class HMF(BaseHMF):
    
    def __init__(self, root_dirpath, memmap_map, verbose=True):
        super(HMF, self).__init__(root_dirpath, memmap_map, verbose)
        
        self.root_dirpath = root_dirpath


        # 0.0.b31 update
        self.arrays = defaultdict(list)

        # self.arrays = list()
        self.str_arrays = list()
        self.node_attrs = list()

        self.dataframe_colnames = defaultdict(dict)
        self.grouped = False

        # 0.0.b31 update
        self.pdfs = dict()
        self.num_pdfs = 0
        self.pdf_names = list()

        self.grouped = dict()
        self.group_sizes = dict()
        self.group_names = dict()
        self.group_items = dict()

        self.current_dataframe_name = None

        self.memmap_map['multi_pdfs'] = False
    
    def from_pandas(self, pdf, groupby=None, orderby=None, ascending=True, group_name=None):
        """
        need to numerify groupby col!"""

        # 0.0.b31 update

        dataframe_name = group_name

        if dataframe_name is None:
            dataframe_name = "{}_{}".format(constants.DATAFRAME_NAME, self.num_pdfs)

        if not dataframe_name in self.pdf_names:
            self.pdf_names.append(dataframe_name)

            self.num_pdfs += 1

        self.pdfs[dataframe_name] = pdf
        self.current_dataframe_name = dataframe_name
        
        if groupby and orderby:
            self.pdfs[dataframe_name][constants.GROUPBY_ENCODER] = self.pdfs[dataframe_name][groupby].astype('category')
            self.pdfs[dataframe_name][constants.GROUPBY_ENCODER] = self.pdfs[dataframe_name][constants.GROUPBY_ENCODER].cat.codes

            self.pdfs[dataframe_name] = self.pdfs[dataframe_name].sort_values(by=[groupby, orderby]).reset_index(drop=True)
            group_array = self.pdfs[dataframe_name][constants.GROUPBY_ENCODER].values

            tmp = pd.DataFrame(self.pdfs[dataframe_name][groupby].unique(), columns=[groupby])
            tmp = tmp.sort_values(by=groupby).reset_index(drop=True)
            group_names = tmp[groupby].tolist()

            self.grouped[dataframe_name] = True
            
        elif orderby:
            self.pdfs[dataframe_name] = self.pdfs[dataframe_name].sort_values(by=[orderby]).reset_index(drop=True)

            group_array = np.zeros(len(self.pdfs[dataframe_name]))
            group_names = [constants.HMF_GROUPBY_DUMMY_NAME]

            self.grouped[dataframe_name] = False
            
        elif groupby:
            self.pdfs[dataframe_name][constants.GROUPBY_ENCODER] = self.pdfs[dataframe_name][groupby].astype('category')
            self.pdfs[dataframe_name][constants.GROUPBY_ENCODER] = self.pdfs[dataframe_name][constants.GROUPBY_ENCODER].cat.codes

            self.pdfs[dataframe_name] = self.pdfs[dataframe_name].sort_values(by=[groupby]).reset_index(drop=True)
            group_array = self.pdfs[dataframe_name][constants.GROUPBY_ENCODER].values

            tmp = pd.DataFrame(self.pdfs[dataframe_name][groupby].unique(), columns=[groupby])
            tmp = tmp.sort_values(by=groupby).reset_index(drop=True)
            group_names = tmp[groupby].tolist()

            self.grouped[dataframe_name] = True
            
        else:
            group_array = np.zeros(len(self.pdfs[dataframe_name]))
            group_names = [constants.HMF_GROUPBY_DUMMY_NAME]

            self.grouped[dataframe_name] = False
            
        border_idx = border_idx_util(group_array)
        group_idx = stride_util(border_idx, 2, 1, np.int32)

        self.group_sizes[dataframe_name] = np.diff(border_idx)
        self.group_names[dataframe_name] = group_names
        self.group_items[dataframe_name] = list(zip(group_names, group_idx))

    def register_array(self, array_filename, columns, encoder=None, decoder=None):
        """Update memmap_map dictionary - which assumes all saves will be successful.
        We need to validity check on arrays
        Also put arrays into sharedctypes
        """
        if(encoder):
            data_array = encoder(self.pdfs[self.current_dataframe_name][columns])
        else:
            data_array = self.pdfs[self.current_dataframe_name][columns].values
            
        self.arrays[self.current_dataframe_name].append((array_filename, data_array))


    def has_groups(self):

        if not self.memmap_map['multi_pdfs']:
            primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
            return self.grouped[primary_default_key]
        else:
            return self.grouped

    def get_group_sizes(self):

        if not self.memmap_map['multi_pdfs']:
            primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
            return self.group_sizes[primary_default_key]
        else:
            return self.group_sizes

    def get_group_names(self):

        if not self.memmap_map['multi_pdfs']:
            primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
            return self.group_names[primary_default_key]
        else:
            return self.group_names

    def get_group_items(self):

        if not self.memmap_map['multi_pdfs']:
            primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
            return {k: np.diff(v)[0] for k, v in self.group_items[primary_default_key]}
        else:
            return {k:{k_: np.diff(v_)[0] for k_, v_ in v} for k, v in self.group_items.items()}

    def get_sorted_group_items(self):

        if not self.memmap_map['multi_pdfs']:
            primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
            return sorted(zip(self.group_names[primary_default_key], self.group_sizes[primary_default_key]), 
                key=lambda x: x[1], 
                reverse=True)
        else:
            return {k:sorted(zip(self.group_names[k], self.group_sizes[k]), 
                key=lambda x: x[1], 
                reverse=True) for k in self.group_names.keys()}

    def get_sorted_group_names(self):

        sorted_group_items = self.get_sorted_group_items()

        if not self.memmap_map['multi_pdfs']:
            return [elem[0] for elem in sorted_group_items]
        else:
            return {k: [elem[0] for elem in sorted_group_items[k]] for k in sorted_group_items.keys()}

    def get_sorted_group_sizes(self):

        sorted_group_items = self.get_sorted_group_items()
        
        if not self.memmap_map['multi_pdfs']:
            return [elem[1] for elem in sorted_group_items]
        else:
            return {k: [elem[1] for elem in sorted_group_items[k]] for k in sorted_group_items.keys()}


    def register_node_attr(self, attr_dirpath, key, value):

        self.node_attrs.append((attr_dirpath, key, value))

    def register_dataframe(self, dataframe_filename, columns):

        if not isinstance(columns, list):
            columns = [columns]

        self.register_array(dataframe_filename, columns)
        self.dataframe_colnames[self.current_dataframe_name][dataframe_filename] = columns


    def get_dataframe(self, dataframe_filepath, idx=None):

        array = self.get_array(dataframe_filepath, idx)
        
        if self.memmap_map['multi_pdfs']:
            dataframe_name = dataframe_filepath.split('/')[1]
        else:
            primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
            dataframe_name = primary_default_key

        dataframe_filename = dataframe_filepath.split('/')[-1]
        columns = self.dataframe_colnames[dataframe_name][dataframe_filename]
        dataframe = pd.DataFrame(array, columns=columns)
        return dataframe


    def set_dataframe(self, dataframe_filepath, pdf, columns):

        # print(self.dataframe_colnames)

        filepath_components = dataframe_filepath.split('/')

        if len(filepath_components) > 2:
            group_name = filepath_components[1]
            
            # if longer, we need to take care of that. Not now.
        
        dataframe_filename = filepath_components[-1]


        if self.memmap_map['multi_pdfs']:

            if len(filepath_components)==3:
                self.dataframe_colnames[group_name][dataframe_filename] = columns
            elif len(filepath_components)==2:
                self.dataframe_colnames[dataframe_filename] = columns

        else:

            self.dataframe_colnames[dataframe_filename] = columns

        self.set_array(dataframe_filepath, pdf[columns].values)
        


    def _write_registered_node_attrs(self):
        """
        The logic used in this method largely mirrors those found in parallel.py.

        Main difference: 
            1. no need to parallelize this
            2. we can rely on baseHMF for this since we have access to the HMF object
        """

        tasks = list(itertools.product(
            range(len(self.node_attrs)), 
            range(len(self.groups))))

        for task in tasks:

            attr_dirpath_standalone = self.node_attrs[task[0]][0]
            key_standalone = self.node_attrs[task[0]][1]
            value_standalone = self.node_attrs[task[0]][2]

            group_name = self.groups[task[1]][0]

            print(group_name, attr_dirpath_standalone, key_standalone, value_standalone)
        
    def close(self, zip_file=False, num_subprocs=None, show_progress=True):
        """
        How we process the str_arrays should depend on how many arrays we have VS how many
        subprocs we can open
        """

        # Record remaining information on memmap_map

        # is multi pdf recorded 
        primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
        if self.num_pdfs>1 or self.current_dataframe_name!=primary_default_key:
            self.memmap_map['multi_pdfs'] = True
        else:
            self.memmap_map['multi_pdfs'] = False

        if(len(self.arrays) > 0):

            if(num_subprocs is None):
                num_subprocs = psutil.cpu_count(logical=False) - 1

            if(self.verbose):
                print('Saving registered arrays using multiprocessing [ {} ] subprocs\n'.format(num_subprocs))

            WPM = WriterProcessManager(self, num_subprocs=num_subprocs, verbose=self.verbose, show_progress=show_progress)
            WPM.start()

            self.failed_tasks = WPM.failed_tasks
            if len(self.failed_tasks) > 0:

                if len(WPM.failure_reasons):
                    warnings.warn(str(WPM.failure_reasons), WRITE_TASK_FAILED)
                    
                if len(WPM.shared_read_error_dict):
                    warnings.warn(str(WPM.shared_read_error_dict), READ_TASK_FAILED)

        memmap_map_dirpath = os.path.join(self.root_dirpath, constants.MEMMAP_MAP_FILENAME)

        fail_safe_save_obj(self.memmap_map, memmap_map_dirpath)

        self.del_pdf()
        self.del_arrays()









    def del_pdf(self):

        try: 
            del self.pdfs
        except Exception as e:
            if not (type(e)==AttributeError):
                raise Exception('failed to delete pdf')


    def del_arrays(self):

        try: 
            del self.arrays
        except Exception as e:
            if not (type(e)==AttributeError):
                raise Exception('failed to delete arrays')



# PR 0.0.b16
def fail_safe_save_obj(obj, dirpath):

    for i in range(constants.NUM_FILE_COPY):

        try:

            copy_dirpath = dirpath + str(i)

            save_obj(obj, copy_dirpath)

        except:

            continue

def fail_safe_load_obj(dirpath):

    for i in range(constants.NUM_FILE_COPY):

        try:

            copy_dirpath = dirpath + str(i)

            return load_obj(copy_dirpath)

        except:

            continue
            
    raise IOError("Damn it, failed to read file again")

def fail_safe_check_obj(root_path, filename):

    file_list = os.listdir(root_path)

    for i in range(constants.NUM_FILE_COPY):

        copy_filename = filename + str(i)

        if copy_filename in file_list:

            return True




            