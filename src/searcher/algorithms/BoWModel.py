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

    def fit(self, corpus):
        self._sparse_matrix = self._vectorizer.fit_transform(corpus).astype('float')

    def transform(self, query):
        return self._vectorizer.transform([query]).astype('float')

    def get_similarity(self, query):
        return cosine_similarity(query, self._sparse_matrix).flatten()
