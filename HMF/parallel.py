from .utils import stride_util
from .utils import border_idx_util
from .utils import write_memmap
from .utils import read_memmap
from .utils import printProgressBar

from multiprocessing import Process, Manager, sharedctypes
import itertools
import numpy as np
from collections import defaultdict
import time

from . import constants

MAX_WRITE_ATTEMPT = 3
READ_WAIT_INTERVALS = [0.1, 1.0, 3.0]
MAX_READ_ATTEMPT = len(READ_WAIT_INTERVALS)


class WriterProcess(Process):
    
    def __init__(self, shared_write_result_dict, shared_write_error_dict, task):
        super(WriterProcess, self).__init__()

        self.shared_write_result_dict = shared_write_result_dict
        self.shared_write_error_dict = shared_write_error_dict
        self.task_key = task

        key, array_idx, group_idx = task
        array_filename = SHARED_HMF_OBJ.arrays[key][array_idx][0]
        group_name = SHARED_HMF_OBJ.group_items[key][group_idx][0]
        start_idx, end_idx = SHARED_HMF_OBJ.group_items[key][group_idx][1]
        self.array = SHARED_HMF_OBJ.arrays[key][array_idx][1][start_idx:end_idx]

        # updated 0.0.b31
        array_filepath = "{}/".format(SHARED_HMF_OBJ.root_dirpath)

        # if there is only one key and its value is constants.DATAFRAME_NAME:
        # array_filepath should not contain the key
        keys = list(SHARED_HMF_OBJ.group_items.keys())
        primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
        if(len(keys)==1 and keys[0]==primary_default_key):
            pass
        else:
            array_filepath += "{}__".format(key)

        # if the key's group is only one and its value is constants.HMF_GROUPBY_DUMMY_NAME:
        # array_filepath should not contain the group name
        if(len(SHARED_HMF_OBJ.group_items[key])==1 and SHARED_HMF_OBJ.group_names[key][0]==constants.HMF_GROUPBY_DUMMY_NAME):
            array_filepath += array_filename
            
        else:
            array_filename = SHARED_HMF_OBJ._assemble_dirpath(group_name, array_filename)
            array_filepath += array_filename
        
        self.array_filepath = array_filepath

        # Note:
        # 1. This is the actual file path
        # 2. "ROOTPATH/PATH__NAME"
        
    def run(self):
        """
        We need a way to surface the failure reasons.
        In case of actual failed task.
        """
        
        # self.write_array(self.array_filepath, self.array)
        
        try:
            # start_time = time.time()
            self.write_array(self.array_filepath, self.array)

            # print(time.time() - start_time)

            self.shared_write_result_dict[self.task_key] = 'success'
            
        except Exception as e:
            
            self.shared_write_error_dict[self.task_key] = str(e)
            self.shared_write_result_dict[self.task_key] = 'failure'
    
    def write_array(self, array_filepath, array):
        
        dtype = str(array.dtype)
        shape = array.shape
        write_memmap(array_filepath, dtype, shape, array)


class WriterProcessManager():
    
    def __init__(self, hmf_obj, num_subprocs=4, verbose=True, show_progress=True):


        # start_time = time.time()
        global SHARED_HMF_OBJ
        SHARED_HMF_OBJ = hmf_obj

        self.hmf_obj = hmf_obj
        
        # update 0.0.b31
        self.tasks = list()
        data_keys = list(hmf_obj.arrays.keys())
        for data_key in data_keys:
            
            arrays = hmf_obj.arrays[data_key]
            group_items = hmf_obj.group_items[data_key]
            
            tasks = list(itertools.product(
                range(len(arrays)), 
                range(len(group_items))))
            tasks = [(data_key, *task) for task in tasks]
            
            self.tasks += tasks

        self.pending_tasks = []
        self.failed_tasks = []
        self.successful_tasks = []
        
        self.write_attempt_dict = defaultdict(int)
        self.read_attempt_dict = defaultdict(int)

        self.successful_write_tasks = []
        self.failed_write_tasks = []

        self.successful_read_tasks = []
        self.failed_read_tasks = []
        
        manager = Manager()
        self.shared_write_result_dict = manager.dict()
        self.shared_write_error_dict = manager.dict()
        self.shared_read_error_dict = dict()
        self.subprocs = []
        
        self.num_subprocs = num_subprocs
        self.verbose = verbose

        self.max_len = len(self.tasks)
        self.show_progress = show_progress
        
    def read_task(self, task):

        # updated 0.0.b31
    
        key, array_idx, group_idx = task

        array_filename = self.hmf_obj.arrays[key][array_idx][0]
        shared_whole_array = self.hmf_obj.arrays[key][array_idx][1]

        group_name = self.hmf_obj.group_items[key][group_idx][0]
        start_idx, end_idx = self.hmf_obj.group_items[key][group_idx][1]

        array_filepath = ''

        # if there is only one key and its value is constants.DATAFRAME_NAME:
        # array_filepath should not contain the key
        keys = list(self.hmf_obj.group_items.keys())
        primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
        if(len(keys)==1 and keys[0]==primary_default_key):
            pass
        else:
            array_filepath = '/'.join((array_filepath, key))

        # if the key's group is only one and its value is constants.HMF_GROUPBY_DUMMY_NAME:
        # array_filepath should not contain the group name
        if(len(self.hmf_obj.group_items[key])==1 and self.hmf_obj.group_names[key][0]==constants.HMF_GROUPBY_DUMMY_NAME):
            array_filepath = '/'.join((array_filepath, array_filename))
        else:
            # array_filename = self.hmf_obj._assemble_dirpath(group_name, array_filename)
            array_filepath = '/'.join((array_filepath, group_name, array_filename))

        # Note:
        # 1. This is the fake file path
        # 2. But it is constructed to mirror the actual filesystem tree structure with / separator
        # 3. "/PATH/NAME"

        try: 
            self.hmf_obj.get_array(array_filepath)
            return('success')
        except Exception as e:
            self.shared_read_error_dict[task] = str(e)
            return('failure')

    def write_task(self, task):
        """Update memmap_map here.

        This logic only supports single level group...
        """

        subproc = WriterProcess(self.shared_write_result_dict, self.shared_write_error_dict,
            task)

        subproc.daemon = True
        subproc.start()
        return subproc

    def update_memmap_map(self, task):

        key, array_idx, group_idx = task

        array_filename = self.hmf_obj.arrays[key][array_idx][0]
        shared_whole_array = self.hmf_obj.arrays[key][array_idx][1]

        group_name = self.hmf_obj.group_items[key][group_idx][0]
        start_idx, end_idx = self.hmf_obj.group_items[key][group_idx][1]

        # array_filename = self.hmf_obj._assemble_dirpath(group_name, array_filename)
        array = np.ctypeslib.as_array(shared_whole_array)[start_idx:end_idx]

        # updated 0.0.b31
        array_filepath = ''

        # if there is only one key and its value is constants.DATAFRAME_NAME:
        # array_filepath should not contain the key
        keys = list(self.hmf_obj.group_items.keys())
        primary_default_key = "{}_{}".format(constants.DATAFRAME_NAME, 0)
        if(len(keys)==1 and keys[0]==primary_default_key):
            pass
        else:
            array_filepath = '/'.join((array_filepath, key))

        # if the key's group is only one and its value is constants.HMF_GROUPBY_DUMMY_NAME:
        # array_filepath should not contain the group name
        if(len(self.hmf_obj.group_items[key])==1 and self.hmf_obj.group_names[key][0]==constants.HMF_GROUPBY_DUMMY_NAME):
            array_filepath = '/'.join((array_filepath, array_filename))
        else:
            # array_filename = self.hmf_obj._assemble_dirpath(group_name, array_filename)
            array_filepath = '/'.join((array_filepath, group_name, array_filename))

        # Note:
        # 1. This is the fake file path
        # 2. But it is constructed to mirror the actual filesystem tree structure with / separator
        # 3. "/PATH/NAME"

        self.hmf_obj.update_memmap_map_array(array_filepath, array)

    def start(self):

        if(self.show_progress):
            printProgressBar(0, self.max_len, prefix = 'Progress:', suffix = '', length = 50)
        
        # pending_tasks: written and either not read or failed to read < MAX_READ_ATTEMPT times
        # tasks: not yet written or failed to read MAX_READ_ATTEMPT times
        while(len(self.pending_tasks) > 0 or len(self.tasks) > 0):

            # if we can run more subprocs and if there are tasks remaining
            # run a task in subproc
            if(len(self.subprocs) < self.num_subprocs and len(self.tasks) > 0):

                # this task will not be put back for writing again 
                # until read is failed MAX_READ_ATTEMPT times
                task = self.tasks.pop()

                if self.verbose:
                    print('#######################################\n')
                    print('Working on {}'.format(task))

                # it is pending until read is successful or failed MAX_READ_ATTEMPT times
                # if read is successful, move this task to successful_tasks
                # if read is failed MAX_READ_ATTEMPT times, move it back to tasks
                # --> will count up the write attempt by 1
                # if write count reaches MAX_WRITE_ATTEMPT, move this task to failed_tasks
                # so we can have failures without crashing the program
                self.pending_tasks.append(task)

                subproc = self.write_task(task)
                self.write_attempt_dict[task] += 1

                self.subprocs.append(subproc)

            if self.verbose:
                print('\nCHECKING WRITE STATUS')
                print('from shared_write_result_dict: {}\n'.format(self.shared_write_result_dict))

            if(len(self.shared_write_result_dict.keys()) > 0):

                for tried_write_task in self.shared_write_result_dict.keys():

                    if self.shared_write_result_dict[tried_write_task] == 'success':

                        if self.verbose:
                            print('Write successful: {}'.format(tried_write_task))
                        self.successful_write_tasks.append(tried_write_task)

                        if self.verbose:
                            print('Dropping {} from shared dict for success'.format(tried_write_task))
                        self.shared_write_result_dict.pop(tried_write_task, None)

                        if self.verbose:
                            print('Updating memmap map')
                        self.update_memmap_map(tried_write_task)

                    elif self.shared_write_result_dict[tried_write_task] == 'failure':

                        if self.verbose:
                            print('Write failed: {}'.format(tried_write_task))

                        # we need to push these back to tasks
                        # since we need to start the process again, 
                        # no need for any wait time 
                        # (there is nothing to wait for! it failed!)
                        self.failed_write_tasks.append(tried_write_task)

                        if self.verbose:
                            print('Dropping {} from shared dict for failure'.format(tried_write_task))
                        self.shared_write_result_dict.pop(tried_write_task, None)


            if self.verbose:
                print('\nCHECKING READ STATUS')
                print('from successful_write_tasks: {}\n'.format(self.successful_write_tasks))
            # we record the failed read tasks here
            # the purpose is to temporarily hold these 
            # and escape while loop over successful write tasks
            # add them back in later

            # these are tasks with successful writes
            # but fails at the read
            # what we want for these:
            # need to try reading some more
            self.failed_read_tasks = []

            while(len(self.successful_write_tasks) > 0):

                successful_write_task = self.successful_write_tasks.pop()

                read_result = self.read_task(successful_write_task)

                self.read_attempt_dict[successful_write_task] += 1

                if read_result == 'success':

                    if self.verbose:
                        print('Read successful: {}'.format(successful_write_task))

                    self.successful_read_tasks.append(successful_write_task)

                elif read_result == 'failure':

                    if self.verbose:
                        print('Read failed: {}'.format(successful_write_task))

                    # since the read was not successful for this written task, 
                    # put the written task back into the tasks list:
                    self.failed_read_tasks.append(successful_write_task)

                    read_attempt_cnt = self.read_attempt_dict[successful_write_task] - 1
                    time.sleep(READ_WAIT_INTERVALS[read_attempt_cnt])

            # throw the failed read tasks back in 
            # for these, we don't want to try to write again
            # we want to read again
            self.successful_write_tasks += self.failed_read_tasks
            self.successful_write_tasks = list(set(self.successful_write_tasks))

            if self.verbose:
                print('\n-------')
                print('successful_write_tasks: ', self.successful_write_tasks)
                print('successful_read_tasks: ', self.successful_read_tasks)

                print('write_attempt_dict: ', self.write_attempt_dict)
                print('read_attempt_dict: ', self.read_attempt_dict)

                # print('shared_write_result_dict', )

            # update pending, successful, failed tasks
            if self.verbose:
                print('\nUPDATE PENDINGS')
                print('pending tasks: {}\n'.format(self.pending_tasks))

            if(len(self.pending_tasks) > 0):

                for _ in range(len(self.pending_tasks)):

                    pending_task = self.pending_tasks.pop()

                    if self.verbose:
                        print('Looking at pending task {}'.format(pending_task))

                    if pending_task in self.failed_write_tasks:

                        if self.verbose:
                            print('    pending --> tasks: {}'.format(pending_task))
                            print('    * write failed\n')

                        # don't append since that will try to do this again right away
                        self.tasks.insert(0, pending_task)
                        self.failed_write_tasks.remove(pending_task)

                    elif pending_task in self.successful_read_tasks:

                        if self.verbose:
                            print('    pending --> successful tasks: {}\n'.format(pending_task))

                        self.successful_tasks.append(pending_task)

                    # elif(self.write_attempt_dict[pending_task] == MAX_WRITE_ATTEMPT 
                    #     and self.read_attempt_dict[pending_task] == MAX_READ_ATTEMPT):
                    elif self.write_attempt_dict[pending_task] == MAX_WRITE_ATTEMPT:

                        if self.verbose:
                            print('    pending --> failed tasks: {}'.format(pending_task))
                            print('    * reached MAX_WRITE_ATTEMPT\n')

                        self.failed_tasks.append(pending_task)

                        try:
                            self.successful_write_tasks.remove(pending_task)
                        except:
                            # none of the write was successful
                            pass 

                    elif self.read_attempt_dict[pending_task] == MAX_READ_ATTEMPT:

                        if self.verbose:
                            print('    pending --> tasks: {}'.format(pending_task))
                            print('    * reached MAX_READ_ATTEMPT\n')

                        self.tasks.insert(0, pending_task)
                        self.read_attempt_dict[pending_task] = 0
                        self.successful_write_tasks.remove(pending_task)

                    else:

                        if self.verbose:
                            print('    nothing to update\n')

                        self.pending_tasks.insert(0, pending_task)

                    if(self.show_progress):

                        cur_len = int(len(self.successful_tasks) + len(self.failed_tasks)) 
                        printProgressBar(cur_len, self.max_len, prefix = 'Progress:', suffix = '', length = 50)
                    
            # check status of subprocs after all the above is finished
            # to give subprocs more time to write
            self.subprocs = [elem for elem in self.subprocs if elem.is_alive()]

            if self.verbose:
                print('SUBPROC STATUS')
                print('number of outstanding tasks: {} ( {} )'.format(len(self.tasks), self.tasks))
                print('number of running procs: {}'.format(len(self.subprocs))); print()

            # if all procs are busy, there is no gain from trying to 
            # assign work to them at the moment
            if(len(self.subprocs) == self.num_subprocs):
                time.sleep(0.01)

            # if number of tasks is 0, there is no gain from trying to
            # assign work to them at the moment
            # also, if subproc is still running, sleep to give it time to finish
            if(len(self.subprocs) > 0 and len(self.tasks) == 0):
                time.sleep(0.01)

        self.failure_reasons = dict(self.shared_write_error_dict)

        if(self.show_progress):
            printProgressBar(self.max_len, self.max_len, prefix = 'Progress:', suffix = 'Completed!', length = 50)
            print()

