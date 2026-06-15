#!/bin/sh
set -eu

# Validate required environment variables
: "${POSTGRES_SEEDS:?ERROR: POSTGRES_SEEDS environment variable is required}"
: "${POSTGRES_PWD:?ERROR: POSTGRES_PWD environment variable is required}"
: "${POSTGRES_USER:?ERROR: POSTGRES_USER environment variable is required}"
: "${POSTGRES_PORT:?ERROR: POSTGRES_PORT environment variable is required}"
: "${POSTGRES_DB:?ERROR: POSTGRES_DB environment variable is required}"
: "${POSTGRES_VISIBILITY_DB:?ERROR: POSTGRES_VISIBILITY_DB environment variable is required}"

setup_postgres() {
  echo 'Creating temporal base DB'

  # Create and setup temporal database
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -pw "${POSTGRES_PWD}" -p "${POSTGRES_PORT}" --db "${POSTGRES_DB}" create
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -pw "${POSTGRES_PWD}" -p "${POSTGRES_PORT}" --db "${POSTGRES_DB}" setup-schema -v 0.0
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -pw "${POSTGRES_PWD}" -p "${POSTGRES_PORT}" --db "${POSTGRES_DB}" update-schema -d /etc/temporal/schema/postgresql/v12/temporal/versioned

  echo 'Creating temporal visibility DB'
  # Create and setup temporal database
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -pw "${POSTGRES_PWD}" -p "${POSTGRES_PORT}" --db "${POSTGRES_VISIBILITY_DB}" create
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -pw "${POSTGRES_PWD}" -p "${POSTGRES_PORT}" --db "${POSTGRES_VISIBILITY_DB}" setup-schema -v 0.0
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -pw "${POSTGRES_PWD}" -p "${POSTGRES_PORT}" --db "${POSTGRES_VISIBILITY_DB}" update-schema -d /etc/temporal/schema/postgresql/v12/visibility/versioned

  echo 'PostgreSQL schema setup complete'
}


setup_postgres