from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("prepare_input") \
    .master("local") \
    .getOrCreate()

sc = spark.sparkContext

raw = sc.wholeTextFiles("hdfs:///data/*.txt")

def parse_doc(filepath_content):
    filepath, content = filepath_content
    filename = filepath.rsplit("/", 1)[-1]
    name = filename.rsplit(".txt", 1)[0]
    parts = name.split("_", 1)
    doc_id = parts[0]
    doc_title = parts[1].replace("_", " ") if len(parts) > 1 else ""
    text = content.replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()
    return f"{doc_id}\t{doc_title}\t{text}"

docs_rdd = raw.map(parse_doc).filter(lambda line: len(line.split("\t", 2)) == 3 and len(line.split("\t", 2)[2].strip()) > 0)

docs_rdd.coalesce(1).saveAsTextFile("hdfs:///input/data")

print(f"Prepared {docs_rdd.count()} documents into /input/data")

spark.stop()
