'''
This module handles the data ingestion.
It reads the data from a csv file and returns a list of dictionaries.
'''
import csv
import pandas as pd


class DataIngestor:
    '''
    Class that reads the data from a csv file and provides methods to
    obtain the data needed for each task.
    Attributes:
        csv_data: list of dictionaries
        csv_path: path to the csv file
        panda_data: pandas dataframe to easily manipulate the data
        questions_best_is_min: list of questions where the best value is the minimum
        questions_best_is_max: list of questions where the best value is the maximum
    '''
    def __init__(self, csv_path: str):
        self.csv_data = []
        self.csv_path = csv_path
        self.panda_data = pd.read_csv(csv_path)

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]

        # read the csv file and store the data in a list of dictionaries
        with open(self.csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.csv_data.append(dict(row))

    def get_states_mean(self, question: str):
        '''
        Calculates the average of the data_value for each state for the given question,
        grouped by state and sorts the values in ascending order.
        '''
        pd_data = self.panda_data[self.panda_data['Question'] == question]. \
            groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=True)

        # converts the pandas dataframe to a dictionary
        return pd_data.to_dict()

    def get_state_mean(self, state: str, question: str):
        '''
        Calculates the average of the data_value for the given state and question
        and returns the result as a dictionary.
        '''
        result = {}

        result[state] = self.panda_data[(self.panda_data['LocationDesc'] == state) \
                & (self.panda_data['Question'] == question)]['Data_Value'].mean()

        return result

    def get_best5(self, question: str):
        '''
        Returns the top 5 states with the best values for the given question.
        '''
        if question in self.questions_best_is_min:
            # if the best value is the minimum, we sort the values in ascending order
            pd_data = self.panda_data[self.panda_data['Question'] == question]. \
                groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=True)

            # returns a dictionary
            return pd_data.head(5).to_dict()

        elif question in self.questions_best_is_max:
            # if the best value is the maximum, we sort the values in descending order
            pd_data = self.panda_data[self.panda_data['Question'] == question]. \
                groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=False)

            # returns a dictionary
            return pd_data.head(5).to_dict()

        return None

    def get_worst5(self, question: str):
        '''
        Similar to best5 but we sort the values in the opposite order.
        '''
        if question in self.questions_best_is_min:
            pd_data = self.panda_data[self.panda_data['Question'] == question]. \
                groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=False)

            return pd_data.head(5).to_dict()
        elif question in self.questions_best_is_max:
            pd_data = self.panda_data[self.panda_data['Question'] == question]. \
                groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=True)

            return pd_data.head(5).to_dict()

        return None

    def get_global_mean(self, question: str):
        '''
        Creating a dictionary with the global average value for the given question.
        '''
        result = {}
        result['global_mean'] = self.panda_data[self.panda_data['Question']== question] \
            ['Data_Value'].mean()

        return result

    def get_diff_from_mean(self, question: str):
        '''
        Returning the difference between the global mean and each's states mean.
        '''
        result = self.get_states_mean(question)
        global_mean = self.get_global_mean(question)['global_mean']

        for state in result:
            result[state] = global_mean - result[state]

        return result

    def get_state_diff_from_mean(self, state: str, question: str):
        '''
        Returning the difference between the global mean and the state's mean.
        '''
        state_mean = self.get_state_mean(state, question)[state]
        global_mean = self.get_global_mean(question)['global_mean']

        result = {}
        result[state] = global_mean - state_mean
        return result

    def get_mean_by_category(self, question: str):
        '''
        Returning the average value for each category for the given question.
        '''
        pd_data = self.panda_data[self.panda_data['Question'] == question].groupby \
            (['LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value']. \
                mean().sort_index()

        # convert tuples keys to strings to be able to put them in a json format later
        result_dict = pd_data.to_dict()
        result = {}

        for key in result_dict:
            new_key = "('" + "', '".join(key) + "')"
            result[new_key] = result_dict[key]

        return result

    def get_state_mean_by_category(self, state: str, question: str):
        '''
        Similar to previous function but for only one certain state.
        '''
        pd_data = self.panda_data[(self.panda_data['Question'] == question) & \
            (self.panda_data['LocationDesc'] == state)].groupby \
            (['StratificationCategory1', 'Stratification1'])['Data_Value'].mean().sort_index()

        result_dict = pd_data.to_dict()
        value_dict = {}

        for key in result_dict:
            new_key = "('" + "', '".join(key) + "')"
            value_dict[new_key] = result_dict[key]

        result = {}
        result[state] = value_dict
        return result


class Task:
    '''
    Class that represents a task that needs to be done.
    We keep track of the question, state, task_id, route, its status and its result.
    '''
    def __init__(self, question: str, state_name: str, task_id: int, route: str):
        self.question = question
        self.state_name = state_name
        self.task_id = task_id
        self.route = route
        self.done = False
        self.result = None
