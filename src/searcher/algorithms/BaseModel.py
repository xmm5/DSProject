import pickle

from abc import abstractmethod


class BaseModel:
    """
    Базовый абстрактный класс.
    Для реализации общего интерфейса поиска для различных алгоритмов.
    """

    def __init__(self, **kwargs):
        pass

    def _save(self, file_path: str, obj: object):
        with open(file_path, mode='wb') as fw:
            pickle.dump(obj, fw)

    def _load(self, file_path: str):
        with open(file_path, mode='rb') as fr:
            return pickle.load(fr)

    def save(self, file_path: str):
        pass

    def load(self, file_path: str):
        return None

    @abstractmethod
    def fit(self, corpus):
        pass

    @abstractmethod
    def transform(self, query):
        pass

    @abstractmethod
    def get_similarity(self, query):
        pass
