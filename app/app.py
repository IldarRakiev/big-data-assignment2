import time
import sys
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pyspark.sql import SparkSession

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"


def wait_for_cassandra(host, max_retries=30, delay=10):
    for attempt in range(1, max_retries + 1):
        try:
            c = Cluster([host])
            s = c.connect()
            s.shutdown()
            c.shutdown()
            print(f"Cassandra is ready (attempt {attempt})")
            return
        except Exception as e:
            print(f"Waiting for Cassandra ({attempt}/{max_retries}): {e}")
            time.sleep(delay)
    print("ERROR: Cassandra did not become ready in time")
    sys.exit(1)


def create_schema(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            doc_title text,
            tf int,
            df int,
            dl int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            doc_title text,
            dl int
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS global_stats (
            id int PRIMARY KEY,
            total_docs int,
            avg_dl double
        )
    """)
    print("Cassandra schema created successfully")


def main():
    wait_for_cassandra(CASSANDRA_HOST)

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    create_schema(session)
    session.set_keyspace(KEYSPACE)

    session.execute("TRUNCATE inverted_index")
    session.execute("TRUNCATE doc_stats")
    session.execute("TRUNCATE global_stats")

    spark = SparkSession.builder \
        .appName("store_index") \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    index_rdd = sc.textFile("hdfs:///indexer/index/part-*")

    def parse_index_line(line):
        parts = line.split("\t")
        if len(parts) != 6:
            return None
        term, doc_id, doc_title, tf, df, dl = parts
        return (term, doc_id, doc_title, int(tf), int(df), int(dl))

    parsed = index_rdd.map(parse_index_line).filter(lambda x: x is not None)

    inv_insert = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, doc_title, tf, df, dl) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )

    records = parsed.collect()
    print(f"Inserting {len(records)} records into inverted_index ...")

    batch_size = 50
    for i in range(0, len(records), batch_size):
        batch = BatchStatement()
        for rec in records[i:i + batch_size]:
            batch.add(inv_insert, rec)
        session.execute(batch)
    print("inverted_index populated")

    doc_insert = session.prepare(
        "INSERT INTO doc_stats (doc_id, doc_title, dl) VALUES (?, ?, ?)"
    )
    doc_stats = parsed.map(lambda r: (r[1], (r[2], r[5]))) \
        .reduceByKey(lambda a, b: a).collect()

    print(f"Inserting {len(doc_stats)} records into doc_stats ...")
    for doc_id, (doc_title, dl) in doc_stats:
        session.execute(doc_insert, (doc_id, doc_title, dl))
    print("doc_stats populated")

    total_docs = len(doc_stats)
    avg_dl = sum(dl for _, (_, dl) in doc_stats) / total_docs if total_docs > 0 else 0.0
    session.execute(
        "INSERT INTO global_stats (id, total_docs, avg_dl) VALUES (%s, %s, %s)",
        (1, total_docs, avg_dl)
    )
    print(f"global_stats: total_docs={total_docs}, avg_dl={avg_dl:.2f}")

    spark.stop()
    session.shutdown()
    cluster.shutdown()
    print("Done storing index to Cassandra")


if __name__ == "__main__":
    main()
