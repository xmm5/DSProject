import json
import os
import pickle
import pandas as pd

from src.api import HHSettings
from src.api.HHApi import HHApi
from src.loader.ResumeLoader import ResumeLoader
from src.loader.VacancyLoader import VacancyLoader
from src.parser.DataParser import DataParser


class Loader:
    def __init__(self, working_dir: str):
        """
        :param working_dir: Директория проекта.
        """

        self._working_dir = working_dir
        self._hh = HHApi(user_agents_file=f'{working_dir}/data/etc/user-agents.txt')

    def _load_categories(self, categories_file_path: str) -> None:
        """
        Скачать и обработать категории вакансий.
        """

        if not os.path.isfile(categories_file_path):
            categories = self._hh.get_professional_roles()

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

    def load(self):
        """
        Функция реализует:
        1. Выгрузку описания открытых вакансий и резюме из площадки "hh.ru".
        2. Предобработку текста описаний вакансий и резюме.
        3. Подготовку датасета и сохрание его на диске в формате pickle.
        """

        # Скачать вакансии для указанных настроек.
        vacancy_loader = VacancyLoader(hh=self._hh, working_dir=self._working_dir)

        for option in HHSettings.HH_PROFESSIONAL_INFO:
            vacancy_loader.load(
                area=option['area'],
                professional_role=option['roles'],
                search_in_days=HHSettings.HH_SEARCH_IN_DAYS,
                max_page=HHSettings.HH_VACANCY_MAX_PAGE)

        # Скачать резюме для указанных названий.
        resume_loader = ResumeLoader(
            hh=self._hh,
            working_dir=self._working_dir,
            threadpool_max_workers=4)

        for title in HHSettings.HH_RESUME_TITLES:
            resume_loader.load(
                text=title,
                area=HHSettings.HH_AREA,
                search_in_days=HHSettings.HH_SEARCH_IN_DAYS,
                max_page=HHSettings.HH_RESUME_MAX_PAGE)

    def parse(self, vacancy_limit: int = 5000, resume_limit: int = 250) -> None:
        """
        Парсинг данных и подготовка датасета.

        :param vacancy_limit: Количество вакансий в DataFrame.
        :param resume_limit: Количество резюме в DataFrame.
        :return:
        """

        categories_file_path = f'{self._working_dir}/data/categories.json'

        self._load_categories(categories_file_path=categories_file_path)

        # Парсим данные.
        parser = DataParser(working_dir=self._working_dir, categories_file_path=categories_file_path)

        # Парсинг вакансий, формирование DataFrame и сохранение его на диск в формате pickle.
        file_path = f'{self._working_dir}/data/vacancy.pkl{pickle.HIGHEST_PROTOCOL}'
        if not os.path.isfile(file_path):
            vacancy_df = parser.get_vacancy_dataframe(limit=vacancy_limit)
            vacancy_df.to_pickle(file_path)

        # Парсинг вакансий, формирование DataFrame и сохранение его на диск в формате pickle.
        file_path = f'{self._working_dir}/data/resume.pkl{pickle.HIGHEST_PROTOCOL}'
        if not os.path.isfile(file_path):
            resume_df = parser.get_resume_dataframe(limit=resume_limit)
            resume_df.to_pickle(file_path)

    def get_vacancy_dataframe(self) -> pd.DataFrame:
        file_path = f'{self._working_dir}/data/vacancy.pkl{pickle.HIGHEST_PROTOCOL}'
        if os.path.isfile(file_path):
            return pd.read_pickle(file_path)
        return pd.DataFrame()

    def get_resume_dataframe(self) -> pd.DataFrame:
        file_path = f'{self._working_dir}/data/resume.pkl{pickle.HIGHEST_PROTOCOL}'
        if os.path.isfile(file_path):
            return pd.read_pickle(file_path)
        return pd.DataFrame()
