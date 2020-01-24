#!/usr/bin/env /usr/local/bin/python3.7
# -*- coding: utf-8 -*-
__author__ = "vipksmosar"
__credits__ = ["vipksmosar"]
__version__ = "1.0.1"
__email__ = "admin@npfx.ru"
__status__ = "Sigma-test"

from jira import JIRA
import pandas as pd
import re
import logging
import argparse


logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
 
arg_parser = argparse.ArgumentParser(description='wiki_publisher')
arg_parser.add_argument('--jira_href', type=str, help="--jira_href 'https://jira.ru', default 'https://jira.ru'", default='https://jira.ru')
arg_parser.add_argument('--filein', type=str, help="--filein 'C:/CSV/HOSTS_VIRT_withScanned.csv'")
arg_parser.add_argument('--LOGIN', type=str, help="--LOGIN ivan.ivanov")
arg_parser.add_argument('--PASSWORD', type=str, help="--PASSWORD Pa$$w0rd")



args = arg_parser.parse_args()

class jira_creator:
    def __init__ (self, jira_href, login, password, file):
        self.jira_href = jira_href
        self.login = login
        self.password = password
        self.file = file
        self.jira = None
        
    def __prepare_df (self, file):
        DF = pd.read_excel(file)
        list_decepen = []    
        DF = DF.rename({'date_start':'customfield_11610', 'date_end':'customfield_11630',\
                     'effect':'customfield_11627', 'target':'customfield_11651',\
                     'mark':'labels', 'manager_ia':'customfield_21400','watcher':'customfield_10909'}, axis=1)
        DF.project = DF.project.apply(lambda x: {'key': x})
        DF.issuetype = DF.issuetype.apply(lambda x: {'name': x})
        DF.labels = DF.labels.apply(lambda x: [x])
        DF.customfield_10909 = DF.customfield_10909.apply(lambda x: [{'name': x}])
        list_df = DF.issuetype.tolist()
        for j, i in enumerate(list_df):
            if i['name']=='Проект':
                list_decepen.append(j)
            elif i['name']=='Веха':
                k = j
                if list_df[j-1] == 'Задача':
                    k = j
                list_decepen.append(k)
            elif i['name']=='Задача':
                list_decepen.append(k)
        DF['hierarchy'] = list_decepen
        list_unique = list(set(list_decepen))
        manager_ia = DF.customfield_21400[0]
        DF = DF.drop('customfield_21400', axis=1)
        return DF, list_unique, manager_ia
    
    def __prepare_time (self, dict_issue):
        tasks_list = []
        print(dict_issue)
        print(type(dict_issue))
        if type(dict_issue) == dict:
            dict_issue['customfield_11610'] = re.search('\d{4}-\d{2}-\d{2}', str(dict_issue['customfield_11610']))[0]
            dict_issue['customfield_11630'] = re.search('\d{4}-\d{2}-\d{2}', str(dict_issue['customfield_11630']))[0]
            #print(dict_issue['customfield_11610'], dict_issue['customfield_11630'])
            return dict_issue
        elif type(dict_issue) == list:
            dict_issue=dict_issue[0]
            dict_issue['customfield_11610'] = re.search('\d{4}-\d{2}-\d{2}', str(dict_issue['customfield_11610']))[0]
            dict_issue['customfield_11630'] = re.search('\d{4}-\d{2}-\d{2}', str(dict_issue['customfield_11630']))[0]
        else:
            raise ('Error type '+str(type(dict_issue)))
    
    def __assign_update(self, key, assignee, manager_ia):
        issue = self.jira.issue(key)
        issue.update(fields={'assignee': {'name':assignee}})
        issue.update(fields={'customfield_21400': {'name':manager_ia}})

    def __project_mark_cell_to (self, key, project_id, mark_id):
        issue = self.jira.issue(key)
        issue.update(fields={'customfield_22700':[str(project_id)]})
        issue.update(fields={'customfield_22701':[str(mark_id)]})

    def __create_project_issue (self, dict_project_issue):   
        project_issue = self.jira.create_issue(fields=dict_project_issue)
        id_project = project_issue.id
        key_project = project_issue.key
        return key_project, id_project
    
    def __create_mark_issue (self, dict_mark_issue):
        mark_issue = self.jira.create_issue(fields=dict_mark_issue)
        id_mark_issue = mark_issue.id
        key_mark_issue = mark_issue.key
        return key_mark_issue, id_mark_issue
    
    def __create_task_issue (self, dict_task_issue):
        task_issue = self.jira.create_issue(fields=dict_task_issue)
        key_task_issue = task_issue.key
        id_task_issue = task_issue.id
        return key_task_issue, id_task_issue
        
    def __jira_auth (self):
        options = {"server": self.jira_href}
        self.jira = JIRA(options, basic_auth=(self.login, self.password))
        return self.jira
    
    def __search_double_issue (self, dict_issue):
        print(dict_issue)
        print(dict_issue['summary'])
        search_by_description = self.jira.search_issues('project='+'IAIT'+' and summary ~'+'"'+dict_issue['summary']+'"')
        if search_by_description:
            print(search_by_description)
            return search_by_description[0].key, search_by_description[0].key
        else:
            if dict_issue['issuetype']['name']=='Проект':
                key, id = self.__create_project_issue(dict_issue)
                return key, id
            elif dict_issue['issuetype']['name']=='Веха':
                key, id = self.__create_project_issue(dict_issue)
                return key, id
            elif dict_issue['issuetype']['name']=='Задача':
                key, id = self.__create_project_issue(dict_issue)
                return key, id
            else:
                raise (dict_issue['issuetype']['name'], ' is unsupport name of issue')
    
    def IAIT_create_project (self):
        if self.jira == None:
            self.jira = self.__jira_auth()
        dict_scheme = {}
        DF, list_unique, manager_ia = self.__prepare_df(self.file)
        DF_project = DF[DF.hierarchy == list_unique[0]].drop(['hierarchy'], axis=1)
        dict_project= DF_project.to_dict('r')
        dict_project = self.__prepare_time(dict_project[0])
        assignee_issue = dict_project.pop('assignee')
        project_key, project_id = self.__search_double_issue(dict_project)
        logging.info('{} is create'.format(project_key))
        print(dict_project)
        self.__assign_update(project_key,assignee_issue, manager_ia)
        for i in list_unique[1:]:
            DF_marks = DF[DF.hierarchy == i]
            dict_marks = DF_marks.iloc[:1,:].drop(['customfield_11627', 'customfield_11651', 'hierarchy'], axis=1).to_dict('r')
            dict_marks = self.__prepare_time(dict_marks[0])
            assignee_issue = dict_marks.pop('assignee')
            mark_key, mark_id = self.__search_double_issue(dict_marks)
            logging.info('{} is create'.format(mark_key))
            print(dict_marks)
            self.__assign_update(mark_key ,assignee_issue, manager_ia)
            self.jira.create_issue_link(type="Иерархия задач", inwardIssue=project_key, outwardIssue=mark_key)
            self.__project_mark_cell_to(mark_key, project_id, mark_id)
            logging.info('{} is create link to {}'.format(mark_key, project_key))
            DF_tasks = DF_marks.iloc[1:,:].drop(['customfield_11627', 'customfield_11651', 'hierarchy'], axis=1)
            DF_tasks["customfield_22701"] = DF_tasks.summary.apply(lambda x: [mark_id])
            DF_tasks["customfield_22700"] = DF_tasks.summary.apply(lambda x: [project_id])
            dict_tasks = DF_tasks.to_dict('r')
            for task in dict_tasks:
                print(task)
                dict_tasks = self.__prepare_time(task)
                assignee_issue = task.pop('assignee')
                print(task)
                task_key = self.__search_double_issue(task)
                logging.info('{} is create'.format(task_key))
                self.__assign_update(task_key ,assignee_issue, manager_ia)
                #self.jira.create_issue_link(type="Иерархия задач", inwardIssue=mark_key, outwardIssue=task_key)
                logging.info('{} is create link to'.format(task_key, mark_key))
            
        
    
create_tasks = jira_creator(args.jira_href, args.LOGIN, args.PASSWORD, args.filein)
create_tasks.IAIT_create_project()
