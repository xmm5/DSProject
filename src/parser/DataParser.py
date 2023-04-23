import json
import os
import pandas as pd

from bs4 import BeautifulSoup

from src.parser.TextTransformer import TextTransformer


class DataParser:
    """
    Парсер описания ваканий и резюме загруженных с сайта hh.ru.
    """

    def __init__(self, working_dir: str, categories_file_path: str = None):
        self._working_dir = working_dir
        self._text_transformer = TextTransformer(working_dir=working_dir)

        # Загрузить категории вакансий если они есть.
        self._categories = None
        if categories_file_path and os.path.isfile(categories_file_path):
            with open(categories_file_path, mode='r', encoding='utf8') as fr:
                self._categories = json.load(fr)

    def _get_category(self, professional_roles: list) -> str:
        """
        :param professional_roles:
        :return:
        """
        if self._categories:
            for key in self._categories.keys():
                for role in professional_roles:
                    if role in self._categories[key]['roles']:
                        return self._categories[key]['name']
        return ''

    def get_resume_dataframe(self, limit: int = None) -> pd.DataFrame:
        counter = 0
        data = []
        base_dir = f'{self._working_dir}/data/resume'
        for root, dirs, filenames in os.walk(base_dir):
            for filename in filenames:
                file_path = f'{root}/{filename}'
                # print(file_path)
                if filename.endswith('.html'):
                    with open(file_path, mode='r', encoding='utf8') as fr:
                        resume_parser = BeautifulSoup(fr, 'html.parser')
                        resume_id = os.path.basename(file_path).split('.')[0]

                        # Формирование описания резюме.
                        item = {}

                        item['id'] = resume_id
                        item['url'] = f'https://hh.ru/resume/{resume_id}?customDomain=1'  # Регион Москва
                        item['title'] = ''
                        item['title_tok'] = ''
                        item['experience'] = ''
                        # item['experience_tok'] = ''

                        try:
                            # Название резюме.
                            result = resume_parser.select(
                                selector='span[data-qa="resume-block-title-position"]')
                            if result:
                                text = result[0].text.strip()
                                item['title'] = text
                                item['title_tok'] = self._text_transformer.transform(text)
                            else:
                                print('[!!] название резюме: is empty or not found')

                            # Опыт работы.
                            result = resume_parser.select(
                                selector='div[data-qa="resume-block-experience"] div[data-qa="resume-block-experience-description"]')
                            if result:
                                # Объединить весь опыт работы в одну строку.
                                text = '\n'.join(s.text.strip() for s in result)
                                item['experience'] = text
                                # item['experience_tok'] = self._text_transformer.transform(text)
                            else:
                                print('[!!] опыт работы: is empty or not found')

                            # Обо мне.
                            result = resume_parser.select(
                                selector='div[data-qa="resume-block-skills"] div[data-qa="resume-block-skills-content"]')
                            if result:
                                text = result[0].text.strip()
                                if text:
                                    # Если значение 'experience' либо 'experience_tok' есть, то
                                    # добавить к нему еще блок данных иначе нужно указать значение.

                                    if item['experience']:
                                        item['experience'] += f'\n{text}'
                                    else:
                                        item['experience'] = text

                                    # if item['experience_tok']:
                                    #     item['experience_tok'] += ' '
                                    #     item['experience_tok'] += self._text_transformer.transform(text)
                                    # else:
                                    #     item['experience_tok'] = self._text_transformer.transform(text)
                            else:
                                print('[!!] обо мне: is empty or not found')

                            # Добавить запись.
                            data.append(item)

                            # Ограничение данных добавляемых в DataFrame.
                            counter += 1

                            if counter > 0 and counter % 250 == 0:
                                if limit:
                                    print(f'{counter} resume processed of {limit}')
                                else:
                                    print(f'{counter} resume processed')

                            if limit and (counter >= limit):
                                return pd.json_normalize(data)
                        except AttributeError as err:
                            print("err: " + str(err))

        # Формируем pandas DataFrame.
        return pd.json_normalize(data)

    def get_vacancy_dataframe(self, limit: int = None) -> pd.DataFrame:
        counter = 0
        data = []
        base_dir = f'{self._working_dir}/data/vacancy'
        for root, dirs, filenames in os.walk(base_dir):
            for filename in filenames:
                file_path = f'{root}/{filename}'
                # print(file_path)
                if filename.endswith('.json'):
                    with open(file_path, mode='r', encoding='utf8') as fr:
                        vacancy = json.load(fr)

                        # Формирование описания вакансии.
                        item = {}

                        item['id'] = vacancy['id']
                        item['url'] = vacancy['alternate_url'] + '?customDomain=1'  # Регион Москва
                        item['title'] = vacancy['name']
                        # item['description'] = vacancy['description']
                        item['title_tok'] = self._text_transformer.transform(vacancy['name'])
                        item['description_tok'] = self._text_transformer.transform(vacancy['description'])

                        if 'professional_roles' in vacancy:
                            professional_roles = [int(role['id']) for role in vacancy['professional_roles']]
                            item['category'] = self._get_category(professional_roles)
                        else:
                            item['category'] = ''

                        # Добавить запись.
                        data.append(item)

                        # Ограничение данных добавляемых в DataFrame.
                        counter += 1

                        if counter > 0 and counter % 250 == 0:
                            if limit:
                                print(f'{counter} vacancy processed of {limit}')
                            else:
                                print(f'{counter} vacancy processed')

                        if limit and (counter >= limit):
                            return pd.json_normalize(data)

        # Формируем pandas DataFrame.
        return pd.json_normalize(data)
