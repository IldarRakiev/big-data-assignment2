echo "Storing index data to Cassandra"

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

python3 app.py

echo "Done storing index to Cassandra"
