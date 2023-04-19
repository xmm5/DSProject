from threading import Lock


class DataLoader:
    """
    Базовый класс для классов VacancyLoader и ResumeLoader.
    """

    def __init__(self, threadpool_enable: bool = True, threadpool_max_workers: int = 2):
        """
        :param threadpool_enable: Вклчить поддержку пула потоков для скачивания.
        :param threadpool_max_workers: Количество потоков для скачивания.
        """
        self.__thread_lock = Lock() if threadpool_enable else None
        self.__threadpool_enable = threadpool_enable
        self.__threadpool_max_workers = threadpool_max_workers

    @property
    def threadpool_enable(self):
        return self.__threadpool_enable

    @property
    def threadpool_max_workers(self):
        return self.__threadpool_max_workers

    def _log(self, message: str):
        """
        Вывод информационных сообщений в лог.

        :param message:
        :return:
        """
        if self.__thread_lock:
            try:
                self.__thread_lock.acquire()
                print(message)
            finally:
                self.__thread_lock.release()
        else:
            print(message)
