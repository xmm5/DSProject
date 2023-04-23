import concurrent
import hashlib
import json
import os
import time
import src.Utils as Utils

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from src.api.HHApi import HHApi
from src.loader.DataLoader import DataLoader


class VacancyLoader(DataLoader):
    """
    Класс загружает и кэширует на диске описания вакансий.
    """

    def __init__(self,
                 hh: HHApi,
                 working_dir: str,
                 threadpool_enable: bool = True,
                 threadpool_max_workers: int = 2):
        """
        :param hh: Ссылка на HHApi.
        :param working_dir: Путь к директории ресурсов.
        :param threadpool_enable: Вклчить поддержку пула потоков для скачивания.
        :param threadpool_max_workers: Количество потоков для скачивания.
        """

        super().__init__(threadpool_enable, threadpool_max_workers)
        self._hh = hh
        self._working_dir = working_dir

    def load(self, max_page: int = None, **kwargs) -> None:
        """
        Загрузка вакансий.

        :param max_page: Сколько скачать страниц, по умолчанию качаем все.
        :param kwargs: Данный параметр хранит настройки для вызова метода HHApi.get_vacancies_list(),
                       чтобы не дублировать параметры метода HHApi.get_vacancies_list() в методе VacancyLoader.load().
        :return:
        """

        dt = datetime.now().strftime('%Y%m%d')
        page = 0

        while True and ((not max_page) or (max_page and max_page >= (page + 1))):
            file_path = f'{self._working_dir}/data/vacancy_pages/{dt}/vacancies_{page:06d}_{dt}.json'
            self._log(file_path)

            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

            vacancies = None

            try:
                vacancies = self._hh.get_vacancies_list(page=page, **kwargs)

                with open(file_path, mode='w', encoding='utf8') as fw:
                    fw.write(json.dumps(vacancies, ensure_ascii=False, indent=2))
            except Exception as e:
                self._log(str(e))

                if os.path.isfile(file_path):
                    os.remove(file_path)

            # Проверить условие выхода из бесконечного цикла.
            if not vacancies:
                break  # Нет данных.

            # Собрать все уникальные ID вакансий (без повторений если они будут).
            vacancy_ids = {vacancy['id'] for vacancy in vacancies['items']}

            if self.threadpool_enable:
                # Загрузка описания вакансий в несколько потоков.
                with ThreadPoolExecutor(max_workers=self.threadpool_max_workers) as pool:
                    futures = []
                    for vacancy_id in vacancy_ids:
                        futures.append(pool.submit(self._load_vacancy, vacancy_id))
                    concurrent.futures.wait(futures)
            else:
                # Скачать список вакансий.
                for vacancy_id in vacancy_ids:
                    self._load_vacancy(vacancy_id=vacancy_id)

            # Обновление счетчиков и проверка условия выхода из бесконечного цикла.

            page += 1
            pages = int(vacancies.get('pages', 0))

            if (pages - page) == 0:
                break

    def _load_vacancy(self, vacancy_id: str) -> None:
        """
        Загрузка вакансии.

        :param vacancy_id:
        :return:
        """

        vacancy_id_hash = hashlib.md5(vacancy_id.encode('utf-8')).hexdigest()
        file_path = f'{self._working_dir}/data/vacancy/{vacancy_id_hash[:2]}/vacancy_{int(vacancy_id):012d}.json'

        if not os.path.isfile(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            try:
                self._log(f'Trying to download vacancy ID={vacancy_id}')
                vacancy = self._hh.get_vacancy(vacancy_id=vacancy_id)

                # Проверка что скачали корректное описание вакансии, т.е.
                # описание вакансии должно содержать поля ('id', 'name', 'description')
                if all(key in vacancy for key in ('id', 'name', 'description')):
                    # Сохранить вакансию.
                    with open(file_path, mode='w', encoding='utf8') as fw:
                        fw.write(json.dumps(vacancy, ensure_ascii=False, indent=2))

                    # Добавить таймаут скачивания вакансий,
                    # иначе hh.ru не дает скачивать вакансии.
                    time.sleep(Utils.get_random_sleep(1.0, 2.0))
                else:
                    self._log(f'Incorrect data received')
            except Exception as e:
                self._log(str(e))

                if os.path.isfile(file_path):
                    os.remove(file_path)
        else:
            self._log(f'Vacancy ID={vacancy_id} already exists')
