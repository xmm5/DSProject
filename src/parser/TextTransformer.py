import os
import re
import nltk

from typing import Union
from nltk.tokenize import RegexpTokenizer
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import WordNetLemmatizer
from nltk.downloader import Downloader
from pymorphy2 import MorphAnalyzer


class TextTransformer:
    """
    Класс реализует

    - фильтрацию текста
        - удаление HTML-тегов
        - удаление HTML-entities
        - удаление ссылок
        - удаление специальных смволов
        - удаление русских и английских стоп-слов
    - токенизацию текста
        - RegexpTokenizer
    - приведение текста к нормальной форме
        - лемматизация для русских и английских слов
        - cтемминг для русских и английских слов
    """

    def __init__(self, base_dir: str = None):
        # Скачать ресурсы nltk в директорию nltk_dir.
        nltk_dir = f'{base_dir}/nltk' if base_dir else '/tmp/nltk'
        os.makedirs(nltk_dir, exist_ok=True)
        # Настройка путь для поиска ресурсы nltk.
        nltk.data.path.append(nltk_dir)
        nltk_download = Downloader()
        # Скачать ресурсы nltk.
        if not nltk_download.is_installed('wordnet', download_dir=nltk_dir):
            nltk_download.download('wordnet', download_dir=nltk_dir)
        if not nltk_download.is_installed('punkt', download_dir=nltk_dir):
            nltk_download.download('punkt', download_dir=nltk_dir)
        if not nltk_download.is_installed('stopwords', download_dir=nltk_dir):
            nltk_download.download('stopwords', download_dir=nltk_dir)

        flags = (re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL)

        self._tk = RegexpTokenizer(r'[а-яёa-z0-9_]+', flags=flags)

        self._lemmatizer_ru = MorphAnalyzer()
        self._lemmatizer_en = WordNetLemmatizer()

        self._stemmer_ru = SnowballStemmer(language='russian')
        self._stemmer_en = SnowballStemmer(language='english')

        self._stopwords_ru = nltk.corpus.stopwords.words('russian')
        self._stopwords_en = nltk.corpus.stopwords.words('english')

        self._re_html_character_entities = re.compile(r'&(?:(?:[a-z0-9]+|#[0-9]+|#x[a-f0-9]+);)', flags=flags)
        self._re_html_tags = re.compile(r'<[^>]+>', flags=flags)  # r'<([^>]+)>'
        self._re_html_link = re.compile(r'https?://\S+', flags=flags)
        self._re_space = re.compile(r'[\s\t]{1,}', flags=flags)

    def transform(self, text: str, split: bool = False) -> Union[str, list]:  # str|list in Python 3.10
        """
        Метод фильтрует и преобразовываем входной текст.
        На выходе строка токенов.
        """

        # Привести все к нижнему регистру.
        text = text.lower()
        # Удалить HTML сущности, например, "&nbsp;" и др.
        text = self._re_html_character_entities.sub('', text)
        # Удалить HTML-теги.
        text = self._re_html_tags.sub('', text)
        # Удалить HTML-ссылки.
        text = self._re_html_link.sub('', text)
        # Заменить несколько пробелов одним.
        text = self._re_space.sub(' ', text)

        max_token_length = 1

        # Разделить текст на токены.
        tokens_filtered = []
        for token in self._tk.tokenize(text):
            w = token.strip()
            # Фильтрация русских и английских стоп-слов плюс ограничение на длину токена.
            if w not in self._stopwords_ru and w not in self._stopwords_en and len(w) > max_token_length:
                # Лемматизация (привести русские и английские слова к нормальной форме слова).
                w = self._lemmatizer_ru.parse(w)[0].normal_form
                w = self._lemmatizer_en.lemmatize(w)
                # Стемминг (выделить основу слова).
                w = self._stemmer_ru.stem(w)
                w = self._stemmer_en.stem(w)
                tokens_filtered.append(w)

        if split:
            return tokens_filtered

        return ' '.join(tokens_filtered)
