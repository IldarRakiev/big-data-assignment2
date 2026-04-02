source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

if [ -f "a.parquet" ]; then
    echo "Found a.parquet -- generating documents from parquet ..."
    hdfs dfs -put -f a.parquet /
    spark-submit prepare_data.py
    echo "Generated documents from parquet"
else
    echo "No a.parquet found -- using pre-existing documents in data/"
fi

echo "Putting data files to HDFS /data ..."
hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/
hdfs dfs -ls /data | head -20

echo "Preparing /input/data from /data ..."
hdfs dfs -rm -r -f /input/data
spark-submit prepare_input.py

echo "Done data preparation"
hdfs dfs -ls /input/data
