FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 

WORKDIR /app

COPY ingest_HW1.py ingest_HW1.py

ENTRYPOINT [ "python", "ingest_HW1.py" ]