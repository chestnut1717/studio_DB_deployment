FROM python:3

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY utils /app/utils
COPY scripts /app/scripts
COPY secret_key /app/secret_key
COPY uwsgi.ini /app
COPY app.py /app

CMD ["uwsgi","uwsgi.ini"]