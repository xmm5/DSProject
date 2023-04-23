import json
import pandas as pd

from flask import Flask, request

from src.Searcher import Searcher


class RESTService:
    def __init__(self, searcher: Searcher):
        self._searcher = searcher
        self._vacancy_df = None
        self._n_top = 10

    def run(self, vacancy_df: pd.DataFrame):
        self._vacancy_df = vacancy_df.copy()

        app = Flask(__name__)

        @app.route('/api/getSimilarVacancies', methods=['POST'])
        def api():
            params = request.json.get('params', {})
            query = params.get('query', '').strip()
            # print(f'query: {query}')
            model = params.get('model', '').strip().lower()
            print(f'model: {model}')

            df = None

            if model == 'bow':
                df = self._searcher.bow_api(vacancy_df=self._vacancy_df, query=query, n_top=self._n_top)
            elif model == 'tfidf':
                df = self._searcher.tfidf_api(vacancy_df=self._vacancy_df, query=query, n_top=self._n_top)
            elif model == 'w2v':
                df = self._searcher.w2v_api(vacancy_df=self._vacancy_df, query=query, n_top=self._n_top)

            records = {}

            if df is not None:
                records = df.to_dict(orient='records')
            else:
                records = {'error': 'unknown model'}

            return app.response_class(
                response=json.dumps(records, ensure_ascii=False),
                mimetype='application/json',
                status=200)

        app.run(host='0.0.0.0', port=8080, debug=False)
