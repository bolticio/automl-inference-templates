FROM amd64/python:3.9.17-slim-bullseye

WORKDIR /app

RUN python3 -m venv venv

ENV PATH="/app/venv/bin:$PATH"


COPY /inference.py /app
COPY /requirements.txt /app

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

EXPOSE 8080

CMD ["python3", "inference.py"]
