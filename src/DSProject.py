import json
import os
import pickle
import pandas as pd

import src.HHSettings as HHSettings

from src.HHApi import HHApi
from src.loader.ResumeLoader import ResumeLoader
from src.loader.VacancyLoader import VacancyLoader
from src.parser.DataParser import DataParser

working_dir = os.getcwd()  # Рабочая директория проекта.

pd.set_option('display.max_columns', None)  # Выводить все колонки.
pd.set_option('display.max_rows', None)  # Выводить все строки.
pd.set_option('display.width', None)  # Не переносить строки при выводе на консоль.
pd.set_option('display.max_colwidth', 512)  # Выводить все больше текста (без ...).

def prepare_data() -> None:
    """
    Функция реализует:
    1. Выгрузку описания открытых вакансий и резюме из площадки "hh.ru".
    2. Предобработку текста описаний вакансий и резюме.
    3. Подготовку датасета и сохрание его на диске в формате pickle.
    """

    # Скачивание данных.

    hh = HHApi(user_agents_file=f'{working_dir}/data/etc/user-agents.txt')

    # Скачать вакансии для указанных настроек.
    vacancy_loader = VacancyLoader(
        hh=hh,
        working_dir=working_dir)
    for option in HHSettings.HH_PROFESSIONAL_INFO:
        vacancy_loader.load(
            area=option['area'],
            professional_role=option['roles'],
            search_in_days=HHSettings.HH_SEARCH_IN_DAYS,
            max_page=1)  # Скачиваем только первую страницу.

    # Скачать резюме для указанных названий.
    resume_loader = ResumeLoader(
        hh=hh,
        working_dir=working_dir,
        threadpool_max_workers=4)
    resume_title = ['экономист', 'аналитик', 'разработчик', 'инженер']
    for title in resume_title:
        resume_loader.load(
            text=title,
            area=HHSettings.HH_AREA,
            search_in_days=HHSettings.HH_SEARCH_IN_DAYS,
            max_page=1)  # Скачиваем только первую страницу поисковой выдачи.

    # Парсинг данных и подготовка датасета.

    categories_file_path = f'{working_dir}/data/categories.json'

    _load_categories(hh=hh, categories_file_path=categories_file_path)

    # Парсим данные.
    parser = DataParser(working_dir=working_dir, categories_file_path=categories_file_path)

    # Парсинг вакансий, формирование DataFrame и сохранение его на диск в формате pickle.
    vacancy_df = None
    vacancy_pickle_file = f'{working_dir}/data/vacancy.pkl{pickle.HIGHEST_PROTOCOL}'
    if not os.path.isfile(vacancy_pickle_file):
        vacancy_df = parser.get_vacancy_dataframe(limit=5)
        # vacancy_df = parser.get_vacancy_dataframe(limit=5000)
        vacancy_df.to_pickle(vacancy_pickle_file)
    else:
        vacancy_df = pd.read_pickle(vacancy_pickle_file)

    print(vacancy_df.head(5))

    # Парсинг вакансий, формирование DataFrame и сохранение его на диск в формате pickle.
    resume_df = None
    resume_pickle_file = f'{working_dir}/data/resume.pkl{pickle.HIGHEST_PROTOCOL}'
    if not os.path.isfile(resume_pickle_file):
        resume_df = parser.get_resume_dataframe(limit=500)
        resume_df.to_pickle(resume_pickle_file)
    else:
        resume_df = pd.read_pickle(resume_pickle_file)

    print(resume_df.head(5))


def _load_categories(hh: HHApi, categories_file_path: str) -> None:
    """
    Скачать и обработать категории вакансий.
    """

    if not os.path.isfile(categories_file_path):
        categories = hh.get_professional_roles()

        # Форматирование категорий в следующий формат:
        # {
        #   "<category_id>": {
        #     "name": "<category_name>",
        #     "roles": [<category_roles>]
        #   },
        #   ...
        # }
        categories = {
            int(cat['id']): {
                'name': cat['name'], 'roles': [
                    int(role['id']) for role in cat['roles']
                ]
            } for cat in categories['categories']
        }

        # Сохранить на диск.
        with open(categories_file_path, mode='w', encoding='utf8') as fw:
            fw.write(json.dumps(categories, ensure_ascii=False, indent=2))


def run() -> None:
    prepare_data()

    # TODO: Поиск данных.
    # TODO: Реализация REST-сервиса.
