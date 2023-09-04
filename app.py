import logging

from clickhouse_driver import Client
from flask import Flask, request, render_template
from celery import Celery

from product_point.parse import Parse

app = Flask(__name__)

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])


@celery.task
def task_parse(category_id, product_id):
    client = Client(host='82.146.62.10',
                    user='izhuravlev',
                    password='12345678',
                    port='9000')
    result = list()
    parse = Parse(client, category_id, product_id, result)
    parse.start()
    parse.join()
    print(parse.result)

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
