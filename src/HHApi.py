import datetime
import os
import random

import requests


class HHApi:
    """
    https://github.com/hhru/api
    """

    def __init__(self, user_agents_file: str = None, verbose: bool = False):
        """
        :param user_agents_file:
        :param verbose:
        """

        self.user_agents_file = user_agents_file
        self.verbose = verbose

    def __get_user_agent(self) -> str:
        """
        Загружает случайный USER_AGENT из файла,
        если файл не определен или не найден возвращает значение по умолчанию.

        :return:
        """

        if self.user_agents_file and os.path.isfile(self.user_agents_file):
            user_agents = []
            with open(self.user_agents_file, mode='r', encoding='utf8') as fr:
                for user_agent in fr:
                    user_agents.append(user_agent.strip())
            return random.choice(user_agents)

        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'

    def __requests_get(self, url, params=None, headers=None, timeout=15):
        """
        Запрос данных по URL.

        :param url:
        :param params:
        :param headers:
        :param timeout:
        :return:
        """

        try:
            res = requests.get(url, params=params, headers=headers, timeout=timeout)

            if self.verbose:
                print(f'[TRACE] {res.url}')

            if 200 != res.status_code:
                print(f"Can't load data from URL {url}, err={res.status_code}")
                return None

            return res
        except Exception as err:
            print("err: " + str(err))

        return None

    def get_professional_roles(self):
        """
        GET https://api.hh.ru/professional_roles

        :return:
        """
        return self.__requests_get(
            'https://api.hh.ru/professional_roles',
            headers={
                'User-Agent': self.__get_user_agent(),
            },
        ).json()

    def get_vacancies_list(self,
                           text=None,
                           area=None,
                           professional_role=None,
                           page=0,
                           per_page=100,
                           search_in_days=1,
                           order_by='publication_time',
                           host='hh.ru'):
        """
        GET /vacancies

        Метод для получения страницы со списком вакансий.

        :param text: Текстовое поле (название вакансии). Переданное значение ищется в полях вакансии, указанных в параметре search_field.
        :param area: Регион. Необходимо передавать id из справочника /areas.
        :param professional_role: Специализация.
        :param page: Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница.
        :param per_page: Количество вакансий на странице. Ограничен значением в 100.
        :param search_in_days: Искать за N дней (date_from = dt_now - N_days, date_to = dt_now).
        :param order_by: Сортировать: По дате изменения.
        :param host: API HeadHunter позволяет получать данные со всех сайтов группы компании HeadHunter. По умолчанию ищем на 'hh.ru'.
        :return:
        """

        date_to = datetime.datetime.now().replace(microsecond=0)
        date_from = (date_to - datetime.timedelta(days=search_in_days)).replace(microsecond=0)

        params = {
            'text': text,
            'area': area,
            'professional_role': professional_role,
            'page': page,
            'per_page': per_page,
            'date_from': date_from.isoformat(),  # '%Y-%m-%dT%H:%M:%S'
            'date_to': date_to.isoformat(),
            'order_by': order_by,
            'host': host,
        }

        return self.__requests_get(
            'https://api.hh.ru/vacancies',
            params=params,
            headers={
                'User-Agent': self.__get_user_agent(),
            }).json()

    def get_vacancy(self, vacancy_id):
        """
        GET /vacancies/{vacancy_id}

        Метод для получения описания вакансии.

        :param vacancy_id: Идентификатор вакансии.
        :return:
        """

        return self.__requests_get(
            f'https://api.hh.ru/vacancies/{vacancy_id}',
            params=None,
            headers={
                'User-Agent': self.__get_user_agent(),
            }).json()

    def get_resume_list(self,
                        text=None,
                        area=1,
                        professional_role=None,
                        page=0,
                        per_page=20,
                        search_in_days=1,
                        host='hh.ru'
                        ):
        """
        Скачивание списка резюме.

        GET https://hh.ru/search/resume?text=<название>&area=<area_id>&customDomain=<area_id>[&param1=value1, ...]

        :param text:
        :param area:
        :param professional_role:
        :param page:
        :param per_page:
        :param search_in_days:
        :param host:
        :return:
        """

        date_to = datetime.datetime.now().replace(microsecond=0)
        date_from = (date_to - datetime.timedelta(days=search_in_days)).replace(microsecond=0)

        params = {
            'text': text,
            'area': area,
            # FIX: Для поиска по указанноме региону, а не тому который определеил hh.ru.
            'customDomain': area,
            'logic': 'normal',  # Настройка расширенного поиска - все слова
            'pos': 'full_text',  # Настройка расширенного поиска - везде
            'professional_role': professional_role,
            'exp_period': 'all_time',
            'exp_company_size': 'any',
            'exp_industry': 'any',
            'relocation': 'living_or_relocation',
            'salary_from': '',
            'salary_to': '',
            'currency_code': 'RUR',
            'age_from': '',
            'age_to': '',
            'gender': 'unknown',
            # Образование: Бакалавр, Магистр, Высшее
            # 'education_level': ['bachelor', 'master', 'higher'],
            # Тип занятости: Полная занятость
            'employment': ['full'],
            # График работы: Полный день, Гибкий график, Удаленная работа
            'schedule': ['fullDay', 'flexible', 'remote'],
            # Сортировка: значение из списка ['relevance', 'salary_asc', 'salary_desc', 'publication_time']
            'order_by': 'publication_time',
            # Интервал поиска: значение из списка [-1, 0, 1, 3, 7, 30, 365]
            'search_period': -1,
            'date_from': date_from.strftime('%d.%m.%Y'),
            'date_to': date_to.strftime('%d.%m.%Y'),
            # Номер страницы [0..n].
            'page': page,
            # Количество резюме на странице [20, 50, 100].
            'items_on_page': per_page,
            'no_magic': 'true',
        }

        return self.__requests_get(
            'https://hh.ru/search/resume',
            params=params,
            headers={
                'User-Agent': self.__get_user_agent(),
            }).content.decode('utf-8')

    def get_resume(self,
                   resume_id: str,
                   area: int = 1):
        """
        GET https://hh.ru/resume/{resume_id}?&area=<area_id>&customDomain=<area_id>&hhtmFrom=resume_search_result

        Скачивание описания резюме.

        :param resume_id: Идентификатор резюме.
        :param area:
        :return:
        """

        return self.__requests_get(
            f'https://hh.ru/resume/{resume_id}',
            params={
                'area': area,
                'customDomain': area,
                'hhtmFrom': 'resume_search_result',
            },
            headers={
                'User-Agent': self.__get_user_agent(),
            }).content.decode('utf-8')
