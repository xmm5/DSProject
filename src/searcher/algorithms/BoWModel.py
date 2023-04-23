from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.searcher.algorithms.BaseModel import BaseModel


class BoWModel(BaseModel):
    """
    Bag of Words
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._vectorizer = CountVectorizer(**kwargs)
        self._sparse_matrix = None

    def save(self, file_path: str):
        super()._save(file_path, self._vectorizer)
        super()._save(f'{file_path}_sparse_matrix', self._sparse_matrix)

    def load(self, file_path: str):
        self._vectorizer = super()._load(file_path)
        self._sparse_matrix = super()._load(f'{file_path}_sparse_matrix')

    def fit(self, corpus):
        self._sparse_matrix = self._vectorizer.fit_transform(corpus).astype('float')

    def transform(self, query):
        return self._vectorizer.transform([query]).astype('float')

    def get_similarity(self, query):
        return cosine_similarity(query, self._sparse_matrix).flatten()
