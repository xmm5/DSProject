import pickle
import numpy as np

from gensim.models import Word2Vec, KeyedVectors
from sklearn.metrics.pairwise import cosine_similarity

from src.searcher.algorithms.BaseModel import BaseModel


class W2VModel(BaseModel):
    """
    Word2vec
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._model = Word2Vec(**kwargs)
        self._sparse_matrix = None

    def save(self, file_path):
        self._model.save(file_path)
        self._model.wv.save(f'{file_path}_wv')
        with open(f'{file_path}_sparse_matrix', mode='wb') as fw:
            pickle.dump(self._sparse_matrix, fw)

    def load(self, file_path):
        self._model = Word2Vec.load(file_path)
        self._model.wv = KeyedVectors.load(f'{file_path}_wv')
        with open(f'{file_path}_sparse_matrix', mode='rb') as fr:
            self._sparse_matrix = pickle.load(fr)

    def fit(self, corpus):
        self._model.build_vocab(corpus_iterable=corpus)
        self._model.train(corpus_iterable=corpus, total_examples=self._model.corpus_count, epochs=self._model.epochs)
        # Собираем все преобразования в одну марицу.
        self._sparse_matrix = np.array([self.transform(s) for s in corpus])

    def transform(self, query):
        vector = np.zeros(self._model.vector_size)

        for word in query:
            if word in self._model.wv:
                vector = np.add(vector, self._model.wv[word])
            else:
                vector = np.add(vector, 0.0)

        return vector

    def get_similarity(self, query):
        return cosine_similarity(np.array([query]), self._sparse_matrix).flatten()
