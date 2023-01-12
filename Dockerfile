FROM python:3.6-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY init.py ./


## Retrieve data from ReCiter and import into ReCiterDB

COPY update/init.py ./
COPY update/retrieveNIH.py ./


RUN pip3 install --no-cache-dir -r requirements.txt


## Update

CMD [ "python3", "./retrieveNIH.py" ]
CMD [ "python3", "./update/retrieveNIH.py" ]
