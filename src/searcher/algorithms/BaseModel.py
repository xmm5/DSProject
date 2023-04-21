from abc import abstractmethod


class BaseModel:
    """
    Базовый абстрактный класс.
    Для реализации общего интерфейса поиска для различных алгоритмов.
    """

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def fit(self, corpus):
        pass

    @abstractmethod
    def transform(self, query):
        pass

    @abstractmethod
    def get_similarity(self, query):
        pass
