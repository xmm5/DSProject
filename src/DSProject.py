import os
import pandas as pd

from src.Loader import Loader
from src.RESTService import RESTService
from src.Searcher import Searcher

working_dir = os.getcwd()  # Рабочая директория проекта.

pd.set_option('display.max_columns', None)  # Выводить все колонки.
pd.set_option('display.max_rows', None)  # Выводить все строки.
pd.set_option('display.width', None)  # Не переносить строки при выводе на консоль.
pd.set_option('display.max_colwidth', 256)  # Выводить все больше текста (без ...).


class DSProject:
    def __init__(self):
        self._loader = Loader(working_dir=working_dir)
        self._searcher = Searcher(working_dir=working_dir)
        self._service = RESTService(searcher=self._searcher)
        self._resume_df = None
        self._vacancy_df = None

    def _on_load(self) -> None:
        # Загрузка данных.
        self._loader.load()

    def _on_parse(self):
        # Парсинг данных.
        self._loader.parse(vacancy_limit=5000, resume_limit=500)

    def _on_search(self):
        # Поиск данных.
        self._resume_df = self._loader.get_resume_dataframe()

        # Значение по умолчанию.
        query = 'руководитель отдела подбора персонала'

        if not self._resume_df.empty:
            # n = 3
            # print(self._resume_df[['id', 'title']].iloc[n:n + 5])

            # Поиск данных, например, для резюме:
            # https://hh.ru/resume/db0fd37e0008c91dcb0039ed1f456f57395a47?customDomain=1
            resume = self._resume_df.loc[self._resume_df['id'] == 'db0fd37e0008c91dcb0039ed1f456f57395a47']

            # Формируем запрос который состоит из названия резуме и опыт работы.
            print('-' * 80)
            if not resume.empty:
                query = ' '.join((
                    resume.iloc[0]['title'],
                    resume.iloc[0]['experience'],
                ))
                print(f'Ожидаем вакансии для резюме: "{resume.iloc[0]["title"]}"')
            else:
                print(f'Ожидаем вакансии для резюме: "{query}"')
        else:
            print(f'Ожидаем вакансии для резюме: "{query}"')

        # Поиск вакансий при помощи различных моделей.
        self._vacancy_df = self._loader.get_vacancy_dataframe()

        print('-' * 80)
        self._searcher.bow(vacancy_df=self._vacancy_df, query=query)

        print('-' * 80)
        self._searcher.tfidf(vacancy_df=self._vacancy_df, query=query)

        print('-' * 80)
        self._searcher.w2v(vacancy_df=self._vacancy_df, query=query)

    def _on_service(self):
        self._vacancy_df = self._loader.get_vacancy_dataframe()

        # Запуск локального REST-сервиса.
        self._service.run(vacancy_df=self._vacancy_df)

    def run(self) -> None:
        # Скачиваем данные один раз, если ножно обновить данные, то
        # следует удалить директорию /data/model и файлы /data/*.pkl5 перед запуском.
        if not os.path.exists(f'{working_dir}/data/model'):
            # Скачать вакансии/резюме.
            # см. HHSettings.HH_VACANCY_MAX_PAGE
            # см. HHSettings.HH_RESUME_MAX_PAGE
            self._on_load()
            # Разбор вакансий/резюме.
            self._on_parse()
            # Индексация данных вакансий.
            self._on_search()

        # Запуск REST-сервиса.
        self._on_service()

