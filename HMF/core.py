from .utils import write_memmap
from .utils import read_memmap
from .utils import load_obj

import os


class BaseHMF():
    
    def __init__(self, root_dirpath, memmap_map, verbose):
        """If root_dirpath exists, load the existing memmap so we can get arrays.
        
        (root)

        attributes
        dirpath
        node_type = root
        nodes
        
            group_name1

                attributes
                dirpath
                node_type = group
                nodes

                    array_name1

                        attributes
                        dirpath
                        node_type = array
                        dtype
                        shape

            group_name2

            group_name3 ...

        Question: why not combine the node name and the dict following it into a single dict?
        - this will in fact make searching thru nodes at nodes key much harder since we can't 
          query it using key name, but entire dictionaries will come up --> forcing us to 
          query using a dictionary.
        
        """
        
        if(memmap_map):

            if(verbose):

                print('Loading from existing HMF')

            self.memmap_map = memmap_map

        else:

            if(verbose):

                print('Creating new HMF')

            self.memmap_map = dict()
            self.memmap_map['attributes'] = dict()
            self.memmap_map['nodes'] = dict()
            self.memmap_map['dirpath'] = root_dirpath
            self.memmap_map['node_type'] = 'root'

        self.verbose = verbose
    
    def set_group(self, group_dirpath):
        """
        Todo: validate group_dirpath
        """
        
        self.update_memmap_map_group(group_dirpath)

        
    def update_memmap_map_group(self, group_dirpath):
        
        memmap_map_pos = self.memmap_map['nodes']
        # dict with node_names of group names / array names

        # iterate through each level of group (horizontal traversal)
        for idx, node_name in enumerate(group_dirpath.split('/')):

            # skip / because root is not part of dict ( (root) )
            if idx==0:

                if(node_name!=''):
                    raise ValueError('Path much start with the root directory "/"')

                continue    

            # if node_name is one of memmap_map_pos keys
            if(node_name in memmap_map_pos):

                if self.verbose:
                    print('{} found in memmap'.format(node_name))

                if(not self._is_group_node(memmap_map_pos, node_name)):

                    if self.verbose:
                        print('{} is not a group node so skipping'.format(node_name))

                    continue

                # take the current level dirpath
                group_dirpath_pos = memmap_map_pos[node_name]['dirpath']

                # move to the groups dict position of the current node_name
                memmap_map_pos = memmap_map_pos[node_name]['nodes']
                
            else:

                if self.verbose:
                    print('{} not found in memmap'.format(node_name))
                
                # if node_name is not part of the key, create the node_name's map position
                memmap_map_pos[node_name] = dict()
                memmap_map_pos[node_name]['nodes'] = dict()  # create node names dict
                memmap_map_pos[node_name]['attributes'] = dict()
                memmap_map_pos[node_name]['node_type'] = 'group'

                # if it's a node_name right after / root position
                # will always visit - setting the group_dirpath_pos always
                if idx==1:

                    # start the dirpath afresh
                    memmap_map_pos[node_name]['dirpath'] = node_name
                    group_dirpath_pos = memmap_map_pos[node_name]['dirpath']

                else:

                    # if it's beyond the level right after root /
                    # the dirpath is combination of all previous node_name dirpaths
                    memmap_map_pos[node_name]['dirpath'] = self._assemble_dirpath(group_dirpath_pos, node_name)

                # update the map position before moving to the next level of node_name (if there is any)
                memmap_map_pos = memmap_map_pos[node_name]['nodes']
                
    def retrieve_memmap_map_pos_group(self, group_dirpath):
        """
        group_pos: the dictionary position where nodes, dirpath, node_type can be queried
        i.e. right below the node names dict level


        check if it is indeed a group

        """
        memmap_map_pos = self.memmap_map['nodes']
        
        for idx, node_name in enumerate(group_dirpath.split('/')):
            
            if idx==0:
                continue
            
            if node_name in memmap_map_pos:
            
                memmap_map_group_pos = memmap_map_pos[node_name]
                memmap_map_pos = memmap_map_group_pos['nodes']

            else:
                raise 
                
        return memmap_map_group_pos

    def set_array(self, array_filepath, array):
        
        # validate array
        # validate dirpath
        
        self.update_memmap_map_array(array_filepath, array)
        
        self.write_array(array_filepath, array)
        
    def update_memmap_map_array(self, array_filepath, array):
        
        array_filepaths = array_filepath.split('/')
        
        group_dirpath = '/'.join(array_filepaths[0:-1])
        array_name = array_filepaths[-1]
        
        if len(array_filepaths) > 2:
            self.update_memmap_map_group(group_dirpath)
            
            memmap_map_group_pos = self.retrieve_memmap_map_pos_group(group_dirpath)
        else:
            memmap_map_group_pos = self.memmap_map
            
        # TODO: check if array_name is already in here
        
        memmap_map_group_pos['nodes'][array_name] = dict()
        memmap_map_group_pos['nodes'][array_name]['node_type'] = 'array'
        memmap_map_group_pos['nodes'][array_name]['attributes'] = dict()
        memmap_map_group_pos['nodes'][array_name]['nodes'] = dict()

        filepath = self._assemble_dirpath(memmap_map_group_pos['dirpath'], array_name)
        memmap_map_group_pos['nodes'][array_name]['dirpath'] = filepath
        
        dtype = str(array.dtype)
        shape = array.shape
        
        memmap_map_group_pos['nodes'][array_name]['dtype'] = dtype
        memmap_map_group_pos['nodes'][array_name]['shape'] = shape

    def retrieve_memmap_map_pos_array(self, array_filepath):
        
        array_filepaths = array_filepath.split('/')
        
        group_dirpath = '/'.join(array_filepaths[0:-1])
        array_name = array_filepaths[-1]
        
        if len(array_filepaths) > 2:
            self.update_memmap_map_group(group_dirpath)
            
            memmap_map_group_pos = self.retrieve_memmap_map_pos_group(group_dirpath)
        else:
            memmap_map_group_pos = self.memmap_map
            
        return memmap_map_group_pos['nodes'][array_name]

    def write_array(self, array_filepath, array):
        
        memmap_map_array_pos = self.retrieve_memmap_map_pos_array(array_filepath)
        
        dtype = memmap_map_array_pos['dtype']
        shape = memmap_map_array_pos['shape']
        filepath = os.path.join(
            self.memmap_map['dirpath'],
            memmap_map_array_pos['dirpath'])
        
        write_memmap(filepath, dtype, shape, array)

    def _assemble_dirpath(self, source_path, dest_path):

        return '__'.join((source_path, dest_path))

    def _is_group_node(self, memmap_map_pos, node_name):

        return memmap_map_pos[node_name]['node_type'] == 'group'

    def _is_array_node(self, memmap_map_pos, node_name):

        return memmap_map_pos[node_name]['node_type'] == 'array'      
    
    def get_array(self, array_filepath, idx=None):
        
        memmap_map_array_pos = self.retrieve_memmap_map_pos_array(array_filepath)
        
        dtype = memmap_map_array_pos['dtype']
        shape = memmap_map_array_pos['shape']
        filepath = os.path.join(
            self.memmap_map['dirpath'],
            memmap_map_array_pos['dirpath'])
        
        return read_memmap(filepath, dtype, shape, idx)

    def set_node_attr(self, attr_dirpath, key, value):


        self.update_memmap_map_group(attr_dirpath)
            
        memmap_map_group_pos = self.retrieve_memmap_map_pos_group(attr_dirpath)

        memmap_map_group_pos['attributes'][key] = value

    def get_node_attr(self, attr_dirpath, key):
        
        memmap_map_group_pos = self.retrieve_memmap_map_pos_group(attr_dirpath)

        return memmap_map_group_pos['attributes'][key]
