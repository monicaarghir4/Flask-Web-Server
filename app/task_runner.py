'''
This module contains the ThreadPool and TaskRunner classes.
'''
from queue import Queue
from threading import Thread, Event
import os
import json
from .data_ingestor import DataIngestor


class ThreadPool:
    '''
    Class that will manage the threads in the thread pool.
    '''
    def __init__(self):
        '''
        Initialize the ThreadPool with the number of threads
        specified in the TP_NUM_OF_THREADS environment variable.
        If the environment variable is not set, use the number of
        CPUs on the machine.
        The ThreadPool will be used to run tasks in parallel.
        The ThreadPool will also create a DataIngestor object to
        ingest the data from the CSV file.
        '''

        if 'TP_NUM_OF_THREADS' in os.environ:
            self.num_threads = int(os.environ['TP_NUM_OF_THREADS'])
        else:
            self.num_threads = os.cpu_count()

        # create a queue to store the tasks
        self.tasks = Queue()
        # create a list to store the tasks so we can iterate through them and manipulate that data
        self.tasks_list = []
        # create an event to signal the threads to stop
        self.graceful_shutdown = Event()
        # create a list to store the threads
        self.pool = []
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

        # create results directory
        os.makedirs('results', exist_ok=True)

    def start(self):
        '''
        Start the threads in the thread pool.
        '''
        for _ in range(self.num_threads):
            task_runner = TaskRunner(self.tasks, self.graceful_shutdown, self.data_ingestor)
            self.pool.append(task_runner)
            task_runner.start()

    def add_task(self, task):
        '''
        Add a task to the ThreadPool's task queue and list.
        '''
        self.tasks.put(task)
        self.tasks_list.append(task)

    def stop(self):
        '''
        Announce the threads to stop and wait for them to finish.
        '''
        self.graceful_shutdown.set()

        for task_runner in self.pool:
            task_runner.stop()

        for task_runner in self.pool:
            task_runner.join()

class TaskRunner(Thread):
    '''
    Class that will run tasks from the ThreadPool's task queue.
    '''
    def __init__(self, tasks, graceful_shutdown, data_ingestor):
        '''
        Class constructor.
        '''
        Thread.__init__(self)

        self.tasks = tasks
        self.graceful_shutdown = graceful_shutdown
        self.data_ingestor = data_ingestor

    def run(self):
        '''
        Infinite loop that will run tasks from the ThreadPool's task queue
        until the graceful_shutdown event is set.
        The function checks the route of the task and calls the appropriate
        method that will handle the data.
        '''
        while True:
            if self.graceful_shutdown.is_set():
                break
            if not self.tasks.empty():
                task = self.tasks.get()
                if task.route == 'states_mean':
                    result = self.data_ingestor.get_states_mean(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'state_mean':
                    result = self.data_ingestor.get_state_mean(task.state_name, task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'best5':
                    result = self.data_ingestor.get_best5(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'worst5':
                    result = self.data_ingestor.get_worst5(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'global_mean':
                    result = self.data_ingestor.get_global_mean(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'diff_from_mean':
                    result = self.data_ingestor.get_diff_from_mean(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'state_diff_from_mean':
                    result = self.data_ingestor.get_state_diff_from_mean(task.state_name, \
                                                                         task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'mean_by_category':
                    result = self.data_ingestor.get_mean_by_category(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'state_mean_by_category':
                    result = self.data_ingestor.get_state_mean_by_category(task.state_name, \
                                                                           task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                else:
                    print("Invalid route")

    def stop(self):
        '''
        Set the graceful_shutdown event.
        '''
        self.graceful_shutdown.set()

    def write_result(self, task, result):
        '''
        Write the result of the task to a JSON file.
        '''
        with open(f"results/job_id_{task.task_id}.json", 'w', encoding='utf-8') as f:
            json.dump(result, f)
