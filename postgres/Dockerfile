FROM postgres:16

COPY ./postgres/schemas /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/*.sh