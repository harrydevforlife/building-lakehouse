

# #!/usr/bin/env bash

# TRY_LOOP="20"

# # Global defaults and back-compat
# : "${AIRFLOW_HOME:="/usr/local/airflow"}"
# : "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
# : "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Local}Executor}"

# # Load DAGs examples (default: Yes)
# if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]; then
#   AIRFLOW__CORE__LOAD_EXAMPLES=False
# fi

# export \
#   AIRFLOW_HOME \
#   AIRFLOW__CORE__EXECUTOR \
#   AIRFLOW__CORE__FERNET_KEY \
#   AIRFLOW__CORE__LOAD_EXAMPLES \

# # Install custom python package if requirements.txt is present
# if [ -e "/requirements.txt" ]; then
#     $(command -v pip) install --user -r /requirements.txt
# fi

# wait_for_port() {
#   local name="$1" host="$2" port="$3"
#   local j=0
#   while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
#     j=$((j+1))
#     if [ $j -ge $TRY_LOOP ]; then
#       echo >&2 "$(date) - $host:$port still not reachable, giving up"
#       exit 1
#     fi
#     echo "$(date) - waiting for $name... $j/$TRY_LOOP"
#     sleep 5
#   done
# }

# # MySQL configuration
# : "${POSTGRES_HOST:="postgres-airflow"}"
# : "${POSTGRES_PORT:="5433"}"
# : "${POSTGRES_USER:="airflow"}"
# : "${POSTGRES_PASSWORD:="airflow"}"
# : "${POSTGRES_DB:="airflow"}"
# : "${POSTGRES_EXTRAS:=""}"

# AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
# export AIRFLOW__CORE__SQL_ALCHEMY_CONN

# # Wait for MySQL
# wait_for_port "PostgreSQL" "$POSTGRES_HOST" "$POSTGRES_PORT"

# case "$1" in
#   webserver)
#     airflow db init 
#     airflow connections add 'spark_master' \
#           --conn-type 'spark' \
#           --conn-host 'spark://spark-master:7077'

#     if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
#       # With the "Local" and "Sequential" executors, the scheduler should run in the same container.
#       airflow scheduler &
#     fi

#     # Create the airflow admin user
#     airflow users create \
#           --username admin \
#           --firstname admin \
#           --lastname admin \
#           --password admin \
#           --role Admin \
#           --email admin@example.com

#     exec airflow webserver
#     ;;
#   worker|scheduler)
#     # Give the webserver time to run initdb.
#     sleep 10
#     exec airflow "$@"
#     ;;
#   *)
#     # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
#     exec "$@"
#     ;;
# esac
