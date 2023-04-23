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
        self._loader.parse(vacancy_limit=5000, resume_limit=250)

    def _on_search(self):
        # Поиск данных.
        self._resume_df = self._loader.get_resume_dataframe()

        # n = 3
        # print(resume_df[['id', 'title']].iloc[n:n + 5])

        # Поиск данных, например, для резюме:
        # https://hh.ru/resume/db0fd37e0008c91dcb0039ed1f456f57395a47?customDomain=1
        resume = self._resume_df.loc[self._resume_df['id'] == 'db0fd37e0008c91dcb0039ed1f456f57395a47']

        # Формируем запрос который состоит из названия резуме и опыт работы.
        query = ' '.join((
            resume.iloc[0]['title'],
            resume.iloc[0]['experience'],
        ))

        # Поиск вакансий при помощи различных моделей.
        self._vacancy_df = self._loader.get_vacancy_dataframe()

        print('-' * 80)
        print(f'Ожидаем вакансии для резюме: "{resume.iloc[0]["title"]}"')

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
        # NOTE: Раскомментировать вызовы методов для включения нужной функциональности.

        # self._on_load()
        self._on_parse()
        # self._on_search()
        self._on_service()

