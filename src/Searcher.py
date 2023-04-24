import os
import time
import pandas as pd

from src.parser.TextTransformer import TextTransformer
from src.searcher.algorithms.BoWModel import BoWModel
from src.searcher.algorithms.TfidfModel import TfidfModel
from src.searcher.algorithms.W2VModel import W2VModel


class Searcher:
    def __init__(self, working_dir: str):
        self._working_dir = working_dir

        # При вычислении общего score учитываем:
        # - 10% сходства названия вакансии
        # - 90% сходства описания вакансии
        self._fields = [
            ('title', 0.1),
            ('description', 0.9),
        ]

        self._text_transformer = TextTransformer(working_dir=working_dir)
        self._max_features = 20000

    @staticmethod
    def _calc_score(df, fields):
        """
        Вычислении общего score.

        Формула:
        score = sum(score[i] * weight[i])
        """

        score = 0.0
        for field, weight in fields:
            score += df[f'{field}_score'] * weight
        return score

    def _print_stat(self, df: pd.DataFrame, n_top: int = 10):
        """
        Формирование и вывод на экран статистики поиска ваканисй (N-топ).
        """

        # Вычисление суммарного score.
        df['score'] = df.apply(lambda x: Searcher._calc_score(x, self._fields), axis=1)
        df.sort_values(by=['score'], ascending=False, inplace=True)

        # Формирование списка полей для вывода статистики.
        fields = ['id', 'title'] + [f'{field}_score' for field, _ in self._fields] + ['score', 'url']
        # Напечатать топ-10 ваканисй.
        print(df[fields][df['score'] > 0.0].head(n_top))

    def _get_n_top(self, df: pd.DataFrame, n_top: int = 10) -> pd.DataFrame:
        df['score'] = df.apply(lambda x: Searcher._calc_score(x, self._fields), axis=1)
        df.sort_values(by=['score'], ascending=False, inplace=True)
        fields = ['id', 'title', 'url', 'score']
        return df[fields][df['score'] > 0.0].iloc[:n_top]

    def bow(self, vacancy_df: pd.DataFrame, query: str) -> None:
        """
        Модель "Bag of Words" в целом работает хорошо, но
        может находить не совсем подходящие вакансии по описанию резюме.
        """

        print('Модель: Bag of Words')

        df = vacancy_df.copy()

        for field, _ in self._fields:
            tm_start = time.time()

            # Инициализация и обучение модели.
            model = BoWModel(ngram_range=(1, 2), max_features=20000)

            # NOTE: При первом запуске происходит создание и кэширование модели (занимает некоторое время).
            file_path = f'{self._working_dir}/data/model/bow/{field}.model'
            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                # Подготовка данных для обучения модели.
                corpus = df[f'{field}_tok'].tolist()
                model.fit(corpus)
                model.save(file_path)
            else:
                model.load(file_path)

            # Подготовка запроса.
            target = model.transform(self._text_transformer.transform(query, split=False))

            # Вычисление score для столбца field.
            df[f'{field}_score'] = model.get_similarity(target)

            tm_elapsed = time.time() - tm_start
            print(f'time: {tm_elapsed:.06f} for "{field}_tok"')

        print()

        self._print_stat(df)

    def bow_api(self, vacancy_df: pd.DataFrame, query: str, n_top: int = 10) -> pd.DataFrame:
        df = vacancy_df.copy()

        for field, _ in self._fields:
            tm_start = time.time()
            model = BoWModel(ngram_range=(1, 2), max_features=20000)
            file_path = f'{self._working_dir}/data/model/bow/{field}.model'
            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                corpus = df[f'{field}_tok'].tolist()
                model.fit(corpus)
                model.save(file_path)
            else:
                model.load(file_path)
            target = model.transform(self._text_transformer.transform(query, split=False))
            df[f'{field}_score'] = model.get_similarity(target)
            tm_elapsed = time.time() - tm_start
            print(f'time: {tm_elapsed:.06f} for "{field}_tok"')

        return self._get_n_top(df, n_top=n_top)

    def tfidf(self, vacancy_df: pd.DataFrame, query: str) -> None:
        """
        Модель "Tf-idf" работает лучше всего и в большинстве случаев
        находит подходящие вакансии по описанию резюме.
        """

        print('Модель: Tf-idf')

        df = vacancy_df.copy()

        for field, _ in self._fields:
            tm_start = time.time()

            # Инициализация и обучение модели.
            model = TfidfModel(smooth_idf=True, use_idf=True, ngram_range=(1, 2), max_features=20000)

            # NOTE: При первом запуске происходит создание и кэширование модели (занимает некоторое время).
            file_path = f'{self._working_dir}/data/model/tfidf/{field}.model'
            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                # Подготовка данных для обучения модели.
                corpus = df[f'{field}_tok'].tolist()
                model.fit(corpus)
                model.save(file_path)
            else:
                model.load(file_path)

            # Подготовка запроса.
            target = model.transform(self._text_transformer.transform(query, split=False))

            # Вычисление score для столбца field.
            df[f'{field}_score'] = model.get_similarity(target)

            tm_elapsed = time.time() - tm_start
            print(f'time: {tm_elapsed:.06f} for "{field}_tok"')

        print()

        self._print_stat(df)

    def tfidf_api(self, vacancy_df: pd.DataFrame, query: str, n_top: int = 10) -> pd.DataFrame:
        df = vacancy_df.copy()

        for field, _ in self._fields:
            tm_start = time.time()
            model = TfidfModel(smooth_idf=True, use_idf=True, ngram_range=(1, 2), max_features=20000)
            file_path = f'{self._working_dir}/data/model/tfidf/{field}.model'
            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                corpus = df[f'{field}_tok'].tolist()
                model.fit(corpus)
                model.save(file_path)
            else:
                model.load(file_path)
            target = model.transform(self._text_transformer.transform(query, split=False))
            df[f'{field}_score'] = model.get_similarity(target)
            tm_elapsed = time.time() - tm_start
            print(f'time: {tm_elapsed:.06f} for "{field}_tok"')

        return self._get_n_top(df, n_top=n_top)

    def w2v(self, vacancy_df: pd.DataFrame, query: str) -> None:
        """
        Модель Word2vec может находить соответствия в вакансиях которые реально не подходят, но их
        описание содержит фрагменты описания резюме.
        """

        print('Модель: Word2vec')

        df = vacancy_df.copy()

        for field, _ in self._fields:
            tm_start = time.time()

            # Инициализация и обучение модели.
            model = W2VModel(vector_size=512, window=5, min_count=1, workers=8, epochs=150)

            # NOTE: При первом запуске происходит создание и кэширование модели (занимает некоторое время).
            file_path = f'{self._working_dir}/data/model/w2v/{field}.model'
            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                # Подготовка данных для обучения модели.
                corpus = [w.split() for w in df[f'{field}_tok'].tolist()]
                model.fit(corpus)
                model.save(file_path)
            else:
                model.load(file_path)

            # Подготовка запроса.
            target = model.transform(self._text_transformer.transform(query, split=True))

            # Вычисление score для столбца field.
            df[f'{field}_score'] = model.get_similarity(target)

            tm_elapsed = time.time() - tm_start
            print(f'time: {tm_elapsed:.06f} for "{field}_tok"')

        print()

        self._print_stat(df)

    def w2v_api(self, vacancy_df: pd.DataFrame, query: str, n_top: int = 10) -> pd.DataFrame:
        df = vacancy_df.copy()

        for field, _ in self._fields:
            tm_start = time.time()
            model = W2VModel(vector_size=512, window=5, min_count=1, workers=8, epochs=100)
            file_path = f'{self._working_dir}/data/model/w2v/{field}.model'
            if not os.path.isfile(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                corpus = [w.split() for w in df[f'{field}_tok'].tolist()]
                model.fit(corpus)
                model.save(file_path)
            else:
                model.load(file_path)
            target = model.transform(self._text_transformer.transform(query, split=True))
            df[f'{field}_score'] = model.get_similarity(target)
            tm_elapsed = time.time() - tm_start
            print(f'time: {tm_elapsed:.06f} for "{field}_tok"')

        return self._get_n_top(df, n_top=n_top)
