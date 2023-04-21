import os
import pandas as pd

from src.Loader import Loader
from src.Searcher import Searcher

working_dir = os.getcwd()  # Рабочая директория проекта.

pd.set_option('display.max_columns', None)  # Выводить все колонки.
pd.set_option('display.max_rows', None)  # Выводить все строки.
pd.set_option('display.width', None)  # Не переносить строки при выводе на консоль.
pd.set_option('display.max_colwidth', 256)  # Выводить все больше текста (без ...).


def run() -> None:
    loader = Loader(working_dir=working_dir, vacancy_limit=5000, resume_limit=250)

    # Загрузка данных.
    # loader.load()

    # Парсинг данных.
    loader.parse()

    vacancy_df = loader.get_vacancy_dataframe()
    resume_df = loader.get_resume_dataframe()

    # n = 3
    # print(resume_df[['id', 'title']].iloc[n:n + 5])

    # Поиск данных, например, для резюме:
    # https://hh.ru/resume/db0fd37e0008c91dcb0039ed1f456f57395a47?customDomain=1
    resume = resume_df.loc[resume_df['id'] == 'db0fd37e0008c91dcb0039ed1f456f57395a47']

    # Формируем запрос который состоит из названия резуме и опыт работы.
    query = ' '.join((
        resume.iloc[0]['title'],
        resume.iloc[0]['experience'],
    ))

    # Поиск вакансий при помощь различных моделей.
    searcher = Searcher(working_dir=working_dir)

    print('-' * 80)
    print(f'Ожидаем вакансии для резюме: "{resume.iloc[0]["title"]}"')

    print('-' * 80)
    searcher.bow(vacancy_df=vacancy_df, query=query)

    print('-' * 80)
    searcher.tfidf(vacancy_df=vacancy_df, query=query)

    print('-' * 80)
    searcher.w2v(vacancy_df=vacancy_df, query=query)
