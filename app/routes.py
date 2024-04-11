from app import webserver
from flask import request, jsonify

import os
import json

from .data_ingestor import Task
import flask

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    # TODO
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    
    new_task = Task(data['question'], None, webserver.job_counter, 'states_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json

    new_task = Task(data['question'], data['state'], webserver.job_counter, 'state_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'best5')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'worst5')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'global_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'diff_from_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], data['state'], webserver.job_counter, 'state_diff_from_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'mean_by_category')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    data = request.json

    new_task = Task(data['question'], data['state'], webserver.job_counter, 'state_mean_by_category')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)

    return jsonify({"job_id": return_value})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

@webserver.route('/api/graceful_shutdown', methods=['POST'])
def graceful_shutdown():
    webserver.tasks_runner.stop()
    return jsonify({"message": "Shutting down gracefully"})

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"JobID is {job_id}")
    # Check if job_id is valid
    # Check if job_id is done and return the result
    # If not, return running status
    if request.method == 'GET':
        for task in webserver.tasks_runner.tasks_list:
            job_id_str = "job_id_" + task.task_id.__str__()
           
            if job_id_str == job_id:
                if task.done:
                    result_content = task.result.get_json() if isinstance(task.result, flask.Response) else task.result
                    return jsonify({
                    # "data": jsonify(task.result), # indent=None, ensure_ascii=False),
                    "data": result_content,
                    "status": "done"
                    })
                else:
                    return jsonify({
                    "status": "running"
                    })
                
        return jsonify({
            "reason": "Invalid job_id",
            "status": "error"
            })
    else:
        return jsonify({"error": "Method not allowed"}), 405
    
@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    if request.method == 'GET':
        jobs_running = 0
        for task in webserver.tasks_runner.tasks_list:
            if not task.done:
                jobs_running += 1
        return jsonify({'num_jobs': jobs_running})
    else:
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/jobs', methods=['GET'])
def get_jobs():
    if request.method == 'GET':
        jobs = {}
        for task in webserver.tasks_runner.tasks_list:
            jobs.append(task.task_id)
            if task.done:
                jobs[task.task_id] = "done"
            else:
                jobs[task.task_id] = "running"

        return jsonify({
            'status': 'done',
            'data': jsonify(jobs)
        })
    else:  
        return jsonify({"error": "Method not allowed"}), 405

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
