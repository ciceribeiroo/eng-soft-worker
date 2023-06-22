FROM python:3.8.10-slim
COPY . /app
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
RUN apt update -y
RUN apt --yes install libsndfile1-dev

CMD python /app/main.py