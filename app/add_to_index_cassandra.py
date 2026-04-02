from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    spark = SparkSession.builder \
        .appName("add_to_index") \
        .master("local") \
        .getOrCreate()
    sc = spark.sparkContext

    new_rdd = sc.textFile("hdfs:///tmp/add_output/part-*")

    def parse_line(line):
        parts = line.split("\t")
        if len(parts) != 6:
            return None
        term, doc_id, doc_title, tf, df, dl = parts
        return (term, doc_id, doc_title, int(tf), int(df), int(dl))

    records = new_rdd.map(parse_line).filter(lambda x: x is not None).collect()

    if not records:
        print("No new records to add.")
        spark.stop()
        session.shutdown()
        cluster.shutdown()
        return

    # For each term in the new doc, we need to recalculate df across the whole corpus.
    # Since this is a single-doc addition, the new df for any term that already exists
    # is old_df + 1. For new terms, df = 1.
    inv_insert = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, doc_title, tf, df, dl) VALUES (?, ?, ?, ?, ?, ?)"
    )

    new_doc_id = records[0][1]
    new_doc_title = records[0][2]
    new_dl = records[0][5]

    for term, doc_id, doc_title, tf, _, dl in records:
        existing = session.execute(
            "SELECT df FROM inverted_index WHERE term = %s LIMIT 1", (term,)
        ).one()
        new_df = (existing.df + 1) if existing else 1

        # Update df for all existing postings of this term
        if existing:
            rows = session.execute(
                "SELECT doc_id FROM inverted_index WHERE term = %s", (term,)
            )
            for r in rows:
                session.execute(
                    "UPDATE inverted_index SET df = %s WHERE term = %s AND doc_id = %s",
                    (new_df, term, r.doc_id)
                )

        session.execute(inv_insert, (term, doc_id, doc_title, tf, new_df, dl))

    # Update doc_stats
    session.execute(
        "INSERT INTO doc_stats (doc_id, doc_title, dl) VALUES (%s, %s, %s)",
        (new_doc_id, new_doc_title, new_dl)
    )

    # Recalculate global_stats from doc_stats
    all_docs = session.execute("SELECT dl FROM doc_stats")
    dls = [r.dl for r in all_docs]
    total_docs = len(dls)
    avg_dl = sum(dls) / total_docs if total_docs > 0 else 0.0

    session.execute(
        "INSERT INTO global_stats (id, total_docs, avg_dl) VALUES (%s, %s, %s)",
        (1, total_docs, avg_dl)
    )

    print(f"Added doc {new_doc_id} ('{new_doc_title}'). "
          f"New corpus: {total_docs} docs, avg_dl={avg_dl:.2f}")

    spark.stop()
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
