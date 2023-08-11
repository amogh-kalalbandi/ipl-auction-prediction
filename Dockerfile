FROM python:3.9.7-slim

RUN pip install -U pip
RUN pip install pipenv

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

WORKDIR /app

COPY ["Pipfile", "Pipfile.lock", "./"]

RUN pipenv install --system --deploy

COPY ["data", "./data"]
COPY ["orchestration", "./orchestration"]
COPY ["tmp", "./tmp"]
COPY ["preprocessor", "./preprocessor"]
COPY ["prefect.yaml", "./prefect.yaml"]
COPY ["entrypoint.sh", "./entrypoint.sh"]

EXPOSE 5000 4200

# ENTRYPOINT ["entrypoint.sh"]
ENTRYPOINT ["tail", "-f", "/dev/null"]
