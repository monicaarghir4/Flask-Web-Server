'''
This module contains the endpoints that will be used by the webserver
'''
from flask import request, jsonify
from app import webserver

from .data_ingestor import Task

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    '''
    Function that provides an example of a POST endpoint
    '''

    # Assuming the request contains JSON data
    data = request.json
    print(f"got data in post {data}")

    # Process the received data
    # For demonstration purposes, just echoing back the received data
    response = {"message": "Received data successfully", "data": data}

    # Sending back a JSON response
    return jsonify(response)


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    '''
    Function that resolves the states mean request
    '''

    # Get request data
    data = request.json

    # Creating the task that will be executed
    new_task = Task(data['question'], None, webserver.job_counter, 'states_mean')

    # Registering the task in the tasks runner
    # Incrementing the job counter
    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    # Sending back a JSON response
    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    '''
    Function that handles the state_mean request
    '''

    data = request.json

    new_task = Task(data['question'], data['state'], webserver.job_counter, 'state_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    '''
    Function that handles the best5 request
    '''

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'best5')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    '''
    Function that handles the worst5 request
    '''

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'worst5')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    '''
    Function that handles the global_mean request
    '''

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'global_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    '''
    Function that handles the diff_from_mean request
    '''

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'diff_from_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    '''
    Function that handles the state_diff_from_mean request
    '''

    data = request.json

    new_task = Task(data['question'], data['state'], webserver.job_counter,
                    'state_diff_from_mean')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    '''
    Function that handles the mean_by_category request
    '''

    data = request.json

    new_task = Task(data['question'], None, webserver.job_counter, 'mean_by_category')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    '''
    Function that handles the state_mean_by_category request
    '''

    data = request.json

    new_task = Task(data['question'], data['state'], webserver.job_counter,
                    'state_mean_by_category')

    webserver.tasks_runner.add_task(new_task)
    webserver.job_counter += 1

    return_value = "job_id_" + str(new_task.task_id)
    return jsonify({"job_id": return_value})


# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    '''
    Function that displays the defined routes.
    '''
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    '''
    Function that stops the webserver gracefully and returns a message to the user.
    '''
    webserver.tasks_runner.stop()
    return jsonify({"message": "Shutting down gracefully"})


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    '''
    Function that returns the result of a task.
    '''

    # Going through the tasks we have
    for task in webserver.tasks_runner.tasks_list:
        job_id_str = "job_id_" + str(task.task_id)

        if job_id_str == job_id:
            # If the task is done, we return its result
            if task.done:
                return jsonify({
                "data": task.result,
                "status": "done"
                })
            # Otherwise, we let the user know the task is still running
            return jsonify({
            "status": "running"
            })

    # If we get here, it means the task is not in the list
    return jsonify({
        "reason": "Invalid job_id",
        "status": "error"
        })


@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    '''
    Function that returns the number of jobs currently running
    '''

    # Count the number of jobs running
    jobs_running = 0
    for task in webserver.tasks_runner.tasks_list:
        if not task.done:
            jobs_running += 1

    return jsonify({'num_jobs': jobs_running})


@webserver.route('/api/jobs', methods=['GET'])
def get_jobs():
    '''
    Function that returns the status of all jobs
    '''

    jobs = {}

    # Get the status of all jobs
    for task in webserver.tasks_runner.tasks_list:
        task_id = "job_id_" + str(task.task_id)
        if task.done:
            jobs[task_id] = "done"
        else:
            jobs[task_id] = "running"

    return jsonify({
        'data': jobs,
        'status': 'done'
    })

def get_defined_routes():
    '''
    Function that returns the list of defined routes
    '''
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
