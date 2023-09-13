import logging
import environ

from clickhouse_driver import Client
from flask import Flask, request, render_template
from celery import Celery

from parse import Parse

app = Flask(__name__)

BASE_DIR = environ.Path(__file__) - 2
env = environ.Env()
environ.Env.read_env(env_file=BASE_DIR('.env'))

REDIS_HOST = env.str('REDIS_HOST', 'REDIS_HOST')
CLICKHOUSE_HOST = env.str('CLICKHOUSE_HOST', 'CLICKHOUSE_HOST')
CLICKHOUSE_USER = env.str('CLICKHOUSE_USER', 'CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = env.str('CLICKHOUSE_PASSWORD', 'CLICKHOUSE_PASSWORD')
CLICKHOUSE_PORT = env.str('CLICKHOUSE_PORT', 'CLICKHOUSE_PORT')

app.config['CELERY_BROKER_URL'] = REDIS_HOST
app.config['CELERY_RESULT_BACKEND'] = REDIS_HOST

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])


@celery.task
def task_parse(category_id, product_id):
    client = Client(host=CLICKHOUSE_HOST,
                    user=CLICKHOUSE_USER,
                    password=CLICKHOUSE_PASSWORD,
                    port=CLICKHOUSE_PORT)
    result = list()
    parse = Parse(client, category_id, product_id, result)
    parse.start()
    parse.join()

    # parsers = [Parse(client, product_id, category_id, result) for product_id, category_id in final_dict]
    # for parse in parsers:
    #    parse.start()
    # for parse in parsers:
    #    parse.join()


@app.route('/', methods=['GET', 'POST'])
def products_parse():
    if request.method == "POST":
        category_id = request.form['category_id']
        product_id = request.form['product_id']

        try:
            task_parse.delay(category_id, product_id)
        except BaseException as exc:
            logging.exception(exc)
            return None
    return render_template('parse.html')


if __name__ == '__main__':
    app.run()
