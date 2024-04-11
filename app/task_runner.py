from queue import Queue
from threading import Thread, Event
import time
import os
from .data_ingestor import *
import json
from flask import jsonify

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
    
        if 'TP_NUM_OF_THREADS' in os.environ:
            self.num_threads = int(os.environ['TP_NUM_OF_THREADS'])
        else:    
            self.num_threads = os.cpu_count()

        self.tasks = Queue()
        self.tasks_list = []
        self.graceful_shutdown = Event()
        self.pool = []
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

        # self.csv_data = []
        # create results directory
        os.makedirs('results', exist_ok=True)

    def start(self):
        for _ in range(self.num_threads):
            task_runner = TaskRunner(self.tasks, self.graceful_shutdown, self.data_ingestor)
            self.pool.append(task_runner)
            task_runner.start()
    
    def add_task(self, task):
        self.tasks.put(task)
        self.tasks_list.append(task)

    def stop(self):
        self.graceful_shutdown.set()

        for task_runner in self.pool:
            task_runner.stop()
            
        for task_runner in self.pool:
            task_runner.join()

class TaskRunner(Thread):
    def __init__(self, tasks, graceful_shutdown, data_ingestor):
        # TODO: init necessary data structures
        Thread.__init__(self)

        self.tasks = tasks
        self.graceful_shutdown = graceful_shutdown
        self.data_ingestor = data_ingestor
        # self.csv_data = csv_data

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            # if not self.tasks.empty():
            #     task = self.tasks.get()
            #     task.run_task()
            # elif self.tasks.empty() and self.graceful_shutdown.is_set():
            #     breakf
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
                    result = self.data_ingestor.get_state_diff_from_mean(task.state_name, task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'mean_by_category':
                    result = self.data_ingestor.get_mean_by_category(task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                elif task.route == 'state_mean_by_category':
                    result = self.data_ingestor.get_state_mean_by_category(task.state_name, task.question)
                    task.result = result
                    task.done = True
                    self.write_result(task, result)
                else:
                    print("Invalid route")

    def stop(self):
        self.graceful_shutdown.set()

    def write_result(self, task, result):
        with open(f"results/job_id_{task.task_id}.json", 'w', encoding='utf-8') as f:
            json.dumps(result)