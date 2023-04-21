from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.searcher.algorithms.BaseModel import BaseModel


class TfidfModel(BaseModel):
    """
    Tf-idf
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._vectorizer = TfidfVectorizer(**kwargs)
        self._sparse_matrix = None

    def fit(self, corpus):
        self._sparse_matrix = self._vectorizer.fit_transform(corpus)

    def transform(self, query):
        return self._vectorizer.transform([query])

    def get_similarity(self, query):
        return cosine_similarity(query, self._sparse_matrix).flatten()
