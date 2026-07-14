# Postgres container
docker run -d --name baldsky-postgres \
  -e POSTGRES_USER=baldsky \
  -e POSTGRES_PASSWORD=baldsky \
  -e POSTGRES_DB=baldsky \
  -p 5432:5432 \
  postgres:16