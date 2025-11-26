# Source Data Setup (PostgreSQL)

1. Ensure PostgreSQL 13+ running and accessible.
2. Create database:
   ```
   createdb adventureworks
   ```
3. Navigate to `data/postgresDBSamples/postgresDBSamples-master/adventureworks`.
4. Load schema + seed data:
   ```
   psql -d adventureworks -f install.sql
   ```
   The script ingests CSV files stored under `adventureworks/data/`.
5. Verify sample tables:
   ```
   psql -d adventureworks -c "\dt"
   ```
6. Create technical columns (if not already present) to support CDC:
   ```
   ALTER TABLE sales.customer ADD COLUMN IF NOT EXISTS modifieddate timestamp DEFAULT now();
   ```
7. Grant Airflow service user read-only access.

Once completed, update Airflow connection variables (`pg_host`, `pg_db`, etc.) to point at this database.

