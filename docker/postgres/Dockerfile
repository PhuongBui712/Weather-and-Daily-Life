FROM postgres:14.0

COPY init_db.sh /docker-entrypoint-initdb.d/init_db.sh

RUN chmod +x /docker-entrypoint-initdb.d/init_db.sh

EXPOSE 5432