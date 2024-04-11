import os
import json
import pandas as pd
import csv
from flask import jsonify


class DataIngestor:
    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path
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

    # def read_data_as_list_of_dicts(self):
        with open(self.csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.csv_data.append(dict(row))

    def get_states_mean(self, question: str):
        pd_data = self.panda_data[self.panda_data['Question'] == question].groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=True)
        return pd_data.to_dict()
    
    def get_state_mean(self, state: str, question: str):
        result = {}
        result[state] = self.panda_data[(self.panda_data['LocationDesc'] == state) & (self.panda_data['Question'] == question)]['Data_Value'].mean()
        return result
    
    def get_best5(self, question: str):
        if question in self.questions_best_is_min:
            pd_data = self.panda_data[self.panda_data['Question'] == question].groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=True)
        
            return pd_data.head(5).to_dict()
        elif question in self.questions_best_is_max:
            pd_data = self.panda_data[self.panda_data['Question'] == question].groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=False)
        
            return pd_data.head(5).to_dict()
        else:
            return None
        
    def get_worst5(self, question: str):
        if question in self.questions_best_is_min:
            pd_data = self.panda_data[self.panda_data['Question'] == question].groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=True)
        
            return pd_data.sort_values(ascending=False).head(5).to_dict()
        elif question in self.questions_best_is_max:
            pd_data = self.panda_data[self.panda_data['Question'] == question].groupby(['LocationDesc'])['Data_Value'].mean().sort_values(ascending=False)
        
            return pd_data.sort_values(ascending=True).head(5).to_dict()
        else:
            return None
        
    def get_global_mean(self, question: str):
        result = {}
        result['global_mean'] = self.panda_data[self.panda_data['Question'] == question]['Data_Value'].mean()
        
        return result
    
    def get_diff_from_mean(self, question: str):
        result = self.get_states_mean(question)
        # calculate the difference between the global mean and state_mean for every state
        global_mean = self.get_global_mean(question)['global_mean']
        for state in result:
            result[state] = global_mean - result[state]

        return result
    
    def get_state_diff_from_mean(self, state: str, question: str):
        state_mean = self.get_state_mean(state, question)[state]
        global_mean = self.get_global_mean(question)['global_mean']
        result = {}
        result[state] = global_mean - state_mean
        return result
    
    def get_mean_by_category(self, question: str):
        pd_data = self.panda_data[self.panda_data['Question'] == question].groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean().sort_index()
        # convert tuples keys to strings
        result_dict = pd_data.to_dict()
        result = {}
        for key in result_dict:
            new_key = "('" + "', '".join(key) + "')"
            result[new_key] = result_dict[key]
        return result
    
    def get_state_mean_by_category(self, state: str, question: str):
        pd_data = self.panda_data[(self.panda_data['Question'] == question) & (self.panda_data['LocationDesc'] == state)].groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean().sort_index()
        # convert tuples keys to strings
        result_dict = pd_data.to_dict()
        value_dict = {}
        for key in result_dict:
            new_key = "('" + "', '".join(key) + "')"
            value_dict[new_key] = result_dict[key]
        
        result = {}
        result[state] = value_dict
        return result

class Task:
    def __init__(self, question: str, state_name: str, task_id: int, route: str):
        self.question = question
        self.state_name = state_name
        self.task_id = task_id
        self.route = route
        self.done = False
        self.result = None
