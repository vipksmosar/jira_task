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
arg_parser.add_argument('--AUTHOR', type=str, help="--AUTHOR ivan.ivanov", default=None)
args = arg_parser.parse_args()

class jira_creator:

    def __init__ (self, jira_href, login, password, file, author=None):    #инициализация класса, получение переменных
        self.jira_href = jira_href
        self.login = login
        self.password = password
        self.file = file
        self.jira = None
        self.effect = 'Выполнение согласованных в рамках отчета мероприятий'
        self.target = 'Учет и контроль выполнения мероприятий по итогам аудита'
        self.author = author

    def __permiss_list (self, permiss_str): #вспомогательная функция для подготовки списка массивов прав доступа
        dicts_list = []
        for i in permiss_str.split(','):
            dicts_list.append({'name':i})
        return dicts_list
        
    def __prepare_df (self, file):   #функция подготовки полученной таблицы к нужному для jira виду
        DF = pd.read_excel(file)
        list_decepen = []    
        DF = DF.rename({'Проект':'project', 'Тип_задачи':'issuetype', 'Заголовок':'summary', 'Описание':'description',\
                        'Дата_Начала':'customfield_11610', 'Планируемая_дата_выполнения':'customfield_11630', 'Срок_исполнения':'duedate',\
                        'Метка':'labels', 'Менеджер_ВА':'customfield_21400', 'Менеджер_проекта':'customfield_11622',\
                        'Доступ_к_задаче':'customfield_10909', 'Ответственный':'assignee', 'Комментарий':'comment'}, axis=1)
        DF['customfield_11627'] = self.effect
        DF['customfield_11651'] = self.target
        DF.project = DF.project.apply(lambda x: {'key': x})
        DF.issuetype = DF.issuetype.apply(lambda x: {'name': x})
        DF.labels = DF.labels.apply(lambda x: x.split(','))
        DF.customfield_10909 = DF.customfield_10909.apply(self.__permiss_list)
        DF.customfield_11610 = DF.customfield_11610.apply(lambda x: re.search('\d{4}-\d{2}-\d{2}', str(x))[0])
        DF.customfield_11630 = DF.customfield_11630.apply(lambda x: re.search('\d{4}-\d{2}-\d{2}', str(x))[0])
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
        manager_project = DF.customfield_11622[0]
        DF = DF.drop(['customfield_21400', 'customfield_11622'], axis=1)
        return DF, list_unique, manager_ia, manager_project

    def __add_comment(self, key, comment):  #добавление комментария
        if type(comment) == str:
            issue = self.jira.issue(key)
            last_comment = issue.fields.customfield_13600
            if len(last_comment)>0:
                try:
                    last_comment = last_comment.split('"')[1]
                    if last_comment == comment:
                        logging.warning('comment in {} not added, because it alredy exists'.format(key))
                    else:
                        self.jira.add_comment(issue=key, body=comment)
                except:
                    logging.warning('comment in {} not added'.format(key))
            else:
                self.jira.add_comment(issue=key, body=comment)

    def __assign_update(self, key, assignee, manager_ia, manager_project):  #функция для назначения задачи на ответственного
        issue = self.jira.issue(key)
        issue.update(fields={'assignee': {'name':assignee}, 'customfield_21400': {'name':manager_ia}, 'customfield_11622': {'name':manager_project}})
        if self.author:
            try:
                issue.update(fields={'reporter':{'name':self.author}})
            except:
                logging.warning('cannot change author in {}'.format(key))

    def __project_mark_cell_to (self, key, project_id, mark_id):  #маркировка проектом и вехой
        issue = self.jira.issue(key)
        issue.update(fields={'customfield_22700':[str(project_id)]})
        issue.update(fields={'customfield_22701':[str(mark_id)]})

    def __create_project_issue (self, dict_project_issue):   #создание проекта
        project_issue = self.jira.create_issue(fields=dict_project_issue)
        id_project = project_issue.id
        key_project = project_issue.key
        logging.info('{} is create'.format(key_project))
        return key_project, id_project
    
    def __create_mark_issue (self, dict_mark_issue):     #создание вехи
        mark_issue = self.jira.create_issue(fields=dict_mark_issue)
        id_mark_issue = mark_issue.id
        key_mark_issue = mark_issue.key
        logging.info('{} is create'.format(key_mark_issue))
        return key_mark_issue, id_mark_issue
    
    def __create_task_issue (self, dict_task_issue):    #создание задачи
        task_issue = self.jira.create_issue(fields=dict_task_issue)
        key_task_issue = task_issue.key
        id_task_issue = task_issue.id
        logging.info('{} is create'.format(key_task_issue))
        return key_task_issue, id_task_issue
        
    def __jira_auth (self): #авторизация
        options = {"server": self.jira_href}
        self.jira = JIRA(options, basic_auth=(self.login, self.password))
        return self.jira
    
    def __search_double_issue (self, dict_issue): #функция проверки существования задачи
        logging.info('check for existence {}'.format(dict_issue['summary']))
        search_by_description = self.jira.search_issues('project='+'IAIT'+' and summary ~'+'"'+dict_issue['summary']+'"')
        if search_by_description:
            logging.info('{} already exist'.format(dict_issue['summary']))
            return search_by_description[0].key, search_by_description[0].id
        else:
            if dict_issue['issuetype']['name']=='Проект':
                key, id = self.__create_project_issue(dict_issue)
                return key, id
            elif dict_issue['issuetype']['name']=='Веха':
                key, id = self.__create_mark_issue(dict_issue)
                return key, id
            elif dict_issue['issuetype']['name']=='Задача':
                key, id = self.__create_task_issue(dict_issue)
                return key, id
            else:
                raise (dict_issue['issuetype']['name'], ' is unsupport name of issue')
    
    def IAIT_create_project (self):   #основное тело для вызова функций и преобразования данных
        if self.jira == None:
            self.jira = self.__jira_auth()
        DF, list_unique, manager_ia, manager_project = self.__prepare_df(self.file)
        DF_project = DF[DF.hierarchy == list_unique[0]].drop(['hierarchy', 'duedate'], axis=1)
        dict_project= DF_project.to_dict('r')[0]
        assignee_issue = dict_project.pop('assignee')
        comment_project = dict_project.pop('comment')
        project_key, project_id = self.__search_double_issue(dict_project)
        self.__assign_update(project_key,assignee_issue, manager_ia, manager_project)
        self.__add_comment(project_key, comment_project)
        for i in list_unique[1:]:
            DF_marks = DF[DF.hierarchy == i]
            dict_marks = DF_marks.iloc[:1,:].drop(['customfield_11627', 'customfield_11651', 'hierarchy', 'duedate'], axis=1).to_dict('r')[0]
            assignee_issue = dict_marks.pop('assignee')
            comment_mark = dict_marks.pop('comment')
            mark_key, mark_id = self.__search_double_issue(dict_marks)
            self.__assign_update(mark_key ,assignee_issue, manager_ia, manager_project)
            self.jira.create_issue_link(type="Иерархия задач", inwardIssue=project_key, outwardIssue=mark_key)
            self.__project_mark_cell_to(mark_key, project_id, mark_id)
            self.__add_comment(mark_key, comment_mark)
            logging.info('{} is create link to {}'.format(mark_key, project_key))
            DF_tasks = DF_marks.iloc[1:,:].drop(['customfield_11627', 'customfield_11651', 'hierarchy'], axis=1)
            DF_tasks["customfield_22701"] = DF_tasks.summary.apply(lambda x: [mark_id])
            DF_tasks["customfield_22700"] = DF_tasks.summary.apply(lambda x: [project_id])
            dict_tasks = DF_tasks.to_dict('r')
            for task in dict_tasks:
                assignee_issue = task.pop('assignee')
                comment_task = task.pop('comment')
                task_key, task_id = self.__search_double_issue(task)
                self.__assign_update(task_key ,assignee_issue, manager_ia, manager_project)
                self.__add_comment(task_key, comment_task)
                logging.info('{} is create link to {}'.format(task_key, mark_key))

create_tasks = jira_creator(args.jira_href, args.LOGIN, args.PASSWORD, args.filein, args.AUTHOR)  #создание класса с аргументами из arparser
create_tasks.IAIT_create_project()  #запуск метода
