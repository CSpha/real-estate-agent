FROM postgres:16
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-psycopg2 && rm -rf /var/lib/apt/lists/*
COPY scripts/backup_postgres.sh scripts/verify_postgres_backup.py scripts/verify_backups_loop.sh /app/scripts/
RUN sed -i 's/\r$//' /app/scripts/backup_postgres.sh /app/scripts/verify_backups_loop.sh
CMD ["bash", "/app/scripts/backup_postgres.sh"]
