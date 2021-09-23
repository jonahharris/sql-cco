#!/bin/bash
SQLCCO_TOP="$(cd "$(dirname "$0")"; pwd -P)"
cd $SQLCCO_TOP

MODEL="${1}"
MODELCHECK=$(ls -w1 ${SQLCCO_TOP}/sql/train/loaders/ | sed 's/\.sql$//g' | grep "^${MODEL}\$")

if [[ -z "${MODEL}" ]]; then
  echo "You must request a model to be trained. Options are:"
  ls -w1 ${SQLCCO_TOP}/sql/train/loaders/ | sed 's/\.sql$//g'
  exit 1
fi

if [[ -z "${MODELCHECK}" ]]; then
  echo "Invalid Model. Valid options are:"
  ls -w1 ${SQLCCO_TOP}/sql/train/loaders/ | sed 's/\.sql$//g'
  exit 1
fi

BASE_SQLFILE="${SQLCCO_TOP}/sql/train/loaders/${MODELCHECK}.sql"
LOAD_SQLFILE="${SQLCCO_TOP}/sql/train/01-load.sql"
cp "${BASE_SQLFILE}" "${LOAD_SQLFILE}"

SQLITEDB=${SQLCCO_TOP}/data/sqlite/sqlcco.db

SQLITE_WORKS=$(echo "select sqlite_version() > '3.35'" | sqlite3)
if [[ ! ${SQLITE_WORKS} == "1" ]]; then
  echo "SQLite >= 3.35 is required to build the database."
  exit 1
fi

rm ${SQLITEDB}
for SQLFILE in $(ls -w1 ${SQLCCO_TOP}/sql/train/0*.sql | sort)
do
  echo "Running ${SQLFILE}..."
  cat ${SQLFILE} | sqlite3 ${SQLITEDB}
  if [ $? -ne 0 ]; then
    exit 1
  fi
done
