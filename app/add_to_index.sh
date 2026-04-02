set -e

if [ -z "$1" ]; then
    echo "Usage: bash add_to_index.sh <path_to_local_file>"
    echo "File must be named <doc_id>_<doc_title>.txt"
    exit 1
fi

FILE_PATH="$1"
FILENAME=$(basename "$FILE_PATH")

if [[ ! "$FILENAME" =~ ^[0-9]+_.+\.txt$ ]]; then
    echo "ERROR: File must match pattern <doc_id>_<doc_title>.txt"
    exit 1
fi

echo "Adding document: $FILENAME"

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# 1. Copy file to HDFS /data
hdfs dfs -put -f "$FILE_PATH" /data/

# 2. Prepare single-doc input for MapReduce
DOC_ID=$(echo "$FILENAME" | cut -d'_' -f1)
DOC_TITLE=$(echo "$FILENAME" | sed "s/^${DOC_ID}_//;s/\.txt$//;s/_/ /g")
DOC_TEXT=$(cat "$FILE_PATH" | tr '\n' ' ' | tr '\r' ' ' | tr '\t' ' ')

hdfs dfs -rm -r -f /tmp/add_input
echo -e "${DOC_ID}\t${DOC_TITLE}\t${DOC_TEXT}" | hdfs dfs -put - /tmp/add_input/doc.txt

# 3. Run MapReduce on just this document
hdfs dfs -rm -r -f /tmp/add_output
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input /tmp/add_input \
    -output /tmp/add_output

# 4. Merge new index entries into Cassandra
python3 add_to_index_cassandra.py

# 5. Update /input/data by re-running prepare_input
hdfs dfs -rm -r -f /input/data
spark-submit prepare_input.py

echo "Document $FILENAME added to index"
