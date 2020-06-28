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
from multiprocessing import sharedctypes

GROUPBY_ENCODER = "__HMF__groupByNumericEncoder"
MEMMAP_MAP_FILENAME = "__HMF__memmapMapName"


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

        if(os.path.exists(root_path)):

            is_hmf_directory_flag, memmap_map = is_hmf_directory(root_path)

            if(is_hmf_directory_flag):

                from_existing = True

            else:

                from_existing = False

                # warn that it is not
                # memmap not there
                # try to recover...

                pass

    hmf = HMF(root_path, memmap_map, verbose)

    return hmf

def is_hmf_directory(root_path):
    """Assuming root_path exists:
    1. check memmap_map exists
    2. check files are all present 

    Needs much improvements...
    """

    file_list = os.listdir(root_path)

    if MEMMAP_MAP_FILENAME not in file_list:
        print('memmap_map not present')
        return(False, None)

    memmap_map = load_obj(os.path.join(root_path, MEMMAP_MAP_FILENAME))

    array_file_list = get_all_array_dirpaths(memmap_map)

    if(not set(array_file_list) < set(file_list)):

        return(False, None)

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
        self.arrays = list()
    
    def from_pandas(self, pdf, groupby=None, orderby=None):
        """
        need to numerify groupby col!"""
        
        self.pdf = pdf

        
        
        if groupby and orderby:
            self.pdf[GROUPBY_ENCODER] = self.pdf[groupby].astype('category')
            self.pdf[GROUPBY_ENCODER] = self.pdf[GROUPBY_ENCODER].cat.codes

            self.pdf = self.pdf.sort_values(by=[groupby, orderby]).reset_index(drop=True)
            group_array = self.pdf[GROUPBY_ENCODER].values

            tmp = pd.DataFrame(pdf[groupby].unique(), columns=[groupby])
            tmp = tmp.sort_values(by=groupby).reset_index(drop=True)
            group_names = tmp[groupby].tolist()
            
        elif orderby:
            self.pdf = self.pdf.sort_values(by=[orderby]).reset_index(drop=True)

            group_array = np.zeros(len(pdf))
            group_names = ['0']
            
        elif groupby:
            self.pdf[GROUPBY_ENCODER] = self.pdf[groupby].astype('category')
            self.pdf[GROUPBY_ENCODER] = self.pdf[GROUPBY_ENCODER].cat.codes

            self.pdf = self.pdf.sort_values(by=[groupby]).reset_index(drop=True)
            group_array = self.pdf[GROUPBY_ENCODER].values

            tmp = pd.DataFrame(pdf[groupby].unique(), columns=[groupby])
            tmp = tmp.sort_values(by=groupby).reset_index(drop=True)
            group_names = tmp[groupby].tolist()
            
        else:
            group_array = np.zeros(len(pdf))
            group_names = ['0']
            
        border_idx = border_idx_util(group_array)
        group_idx = stride_util(border_idx, 2, 1, np.int32)

        self.groups = list(zip(group_names, group_idx))

    def register_array(self, array_filename, col_names, encoder=None, decoder=None):
        """Update memmap_map dictionary - which assumes all saves will be successful.
        We need to validity check on arrays
        Also put arrays into sharedctypes
        """

        data_array = self.pdf[col_names].values
        data_array = np.ascontiguousarray(data_array)
        
        if encoder:
            data_array = encoder(data_array)

        
        # sharedctype
            
        tmp = np.ctypeslib.as_ctypes(data_array)
        shared_data_array = sharedctypes.Array(tmp._type_, tmp, lock=False)
        
        self.arrays.append((array_filename, shared_data_array))
        
    def close(self):

        if(len(self.arrays) > 0):

            if(self.verbose):
                print('Saving registered arrays using multiprocessing\n')

            WPM = WriterProcessManager(self, verbose=self.verbose)
            WPM.start()

        memmap_map_dirpath = os.path.join(self.root_dirpath, MEMMAP_MAP_FILENAME)

        save_obj(self.memmap_map, memmap_map_dirpath)
            






