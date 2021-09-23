#!/bin/bash
SQLCCO_TOP="$(cd "$(dirname "$0")"; pwd -P)"
cd $SQLCCO_TOP


OLD_SQLITEDB=${SQLCCO_TOP}/data/sqlite/sqlcco.db
NEW_SQLITEDB=${SQLCCO_TOP}/data/sqlite/sqlcco-deploy.db

SQLITE_WORKS=$(echo "select sqlite_version() > '3.35'" | sqlite3)
if [[ ! ${SQLITE_WORKS} == "1" ]]; then
  echo "SQLite >= 3.35 is required to build the database."
  exit 1
fi

cp ${OLD_SQLITEDB} ${NEW_SQLITEDB}
for SQLFILE in $(ls -w1 ${SQLCCO_TOP}/sql/deploy/0*.sql | sort)
do
  echo "Running ${SQLFILE}..."
  cat ${SQLFILE} | sqlite3 ${NEW_SQLITEDB}
  if [ $? -ne 0 ]; then
    exit 1
  fi
done
