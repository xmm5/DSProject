import concurrent
import hashlib
import os
import re
import time

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from bs4 import BeautifulSoup

from src import Utils
from src.api.HHApi import HHApi
from src.loader.DataLoader import DataLoader


class ResumeLoader(DataLoader):
    """
    Класс загружает и кэширует на диске описания резюме.
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

    def _get_total_pages(self, html: str) -> int:
        """
        Метод возвращает общее количество страниц.
        """

        bs = BeautifulSoup(html, 'html.parser')
        elements = bs.select('div[class="pager"] a[href^="/search/resume"]')
        pages = set()
        for element in elements:
            # Получить ID из URL резюме.
            href_attr = element['href']
            # Получить список параметров ключ=значение из URL резюме.
            result = re.findall(pattern=r'[\?&]{1}([0-9_a-z]+)=([0-9_a-z]+)',
                                string=href_attr,
                                flags=(re.MULTILINE | re.DOTALL | re.UNICODE | re.IGNORECASE))
            if result:
                # Собрать все номера страниц.
                for item in result:
                    if item[0] == 'page':
                        pages.add(int(item[1]))

        # Вернуть максимальный номер страницы.
        return max(pages)

    def load(self, text: str, max_page: int = None, **kwargs) -> None:
        """
        Скачивание резюме.

        :param text: Ключевые слова в названии резюме.
        :param max_page: Сколько скачать страниц, по умолчанию качаем все.
        :param kwargs: Данный параметр хранит настройки для вызова метода HHApi.get_vacancies_list(),
                       чтобы не дублировать параметры метода HHApi.get_vacancies_list() в методе VacancyLoader.load().
        :return:
        """

        dt = datetime.now().strftime('%Y%m%d')
        page = 0
        total_pages = -1

        while True and ((not max_page) or (max_page and max_page >= (page + 1))):
            html = ''
            file_path = f'{self._working_dir}/data/resume_pages/{dt}/rp_{page:06d}_{dt}.html'
            self._log(file_path)

            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

            try:
                html = self._hh.get_resume_list(text, page=page, **kwargs)
                with open(file_path, mode='w', encoding='utf8') as fw:
                    fw.write(html)
            except Exception as e:
                self._log(str(e))

                if os.path.isfile(file_path):
                    os.remove(file_path)

            if total_pages == -1:
                # Получить количество страниц (запрашиваем один раз).
                total_pages = self._get_total_pages(html)

            # Подготовка к скачиванию резюме.
            resume_ids = self._get_resume_ids(html)

            if self.threadpool_enable:
                # Загрузка описания резюме в несколько потоков.
                with ThreadPoolExecutor(max_workers=self.threadpool_max_workers) as pool:
                    futures = []
                    for resume_id in resume_ids:
                        futures.append(pool.submit(self._load_resume, resume_id))
                    concurrent.futures.wait(futures)
            else:
                # Скачать список резюме.
                for resume_id in resume_ids:
                    self._load_resume(resume_id=resume_id)

            # Обновление счетчиков и проверка условия выхода из бесконечного цикла.

            if page > total_pages:
                break

            page += 1

    def _get_resume_ids(self, resume_html: str):
        resume_ids = []
        bs = BeautifulSoup(resume_html, 'html.parser')
        # Поиск ссылок на резюме и подготовка списка ID резюме.
        elements = bs.select('main[class="resume-serp-content"] a[href^="/resume/"]')
        for element in elements:
            # Получить ID резюме.
            href_attr = element['href']
            resume_id = href_attr[href_attr.rindex('/') + 1:href_attr.index('?')]
            resume_ids.append(resume_id)
        return resume_ids

    def _load_resume(self, resume_id: str) -> None:
        """
        Загрузка резюме.

        :param resume_id:
        :return:
        """
        resume_id_hash = hashlib.md5(resume_id.encode('utf-8')).hexdigest()
        file_path = f'{self._working_dir}/data/resume/{resume_id_hash[:2]}/{resume_id}.html'

        if not os.path.isfile(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            try:
                self._log(f'trying to download resume ID={resume_id}')
                resume = self._hh.get_resume(resume_id=resume_id)

                # Сохранить резюме.
                with open(file_path, mode='w', encoding='utf8') as fw:
                    fw.write(resume)

                time.sleep(Utils.get_random_sleep(0.0, 0.1))
            except Exception as e:
                self._log(str(e))

                if os.path.isfile(file_path):
                    os.remove(file_path)
        else:
            self._log(f'Resume ID={resume_id} already exists')
