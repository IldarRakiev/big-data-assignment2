echo "Creating index using MapReduce pipelines"

INPUT_PATH=${1:-/input/data}
echo "Input path: $INPUT_PATH"

hdfs dfs -rm -r -f /indexer/index

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output /indexer/index

echo "Index created at /indexer/index"
hdfs dfs -ls /indexer/index
hdfs dfs -cat /indexer/index/part-* | head -20
