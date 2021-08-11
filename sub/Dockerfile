FROM python:3.9.6-slim
COPY . /sub
WORKDIR /sub
RUN apt-get update \
    && apt-get install -y g++ gcc\
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install . \
    && apt-get purge -y --auto-remove g++ gcc\
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base
WORKDIR /
