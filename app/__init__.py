'''
This file initializes the Flask application and creates the ThreadPool object to run the tasks.
'''
from flask import Flask
from app.task_runner import ThreadPool

# initializing flask app
webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

webserver.tasks_runner.start()

webserver.job_counter = 1

from app import routes
