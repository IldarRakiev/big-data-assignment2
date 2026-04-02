echo "Running full indexing pipeline"

bash create_index.sh "$1"

bash store_index.sh

echo "Indexing pipeline complete"
