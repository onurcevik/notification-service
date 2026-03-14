FROM postgres:16-alpine
# Build context is the repo root (set in docker-compose).
# Postgres runs every *.sql file in /docker-entrypoint-initdb.d alphabetically on first boot.
COPY migrations/*.up.sql /docker-entrypoint-initdb.d/
