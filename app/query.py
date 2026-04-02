import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"
K1 = 1.2
B = 0.75


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py \"your query\"")
        sys.exit(1)

    query_text = " ".join(sys.argv[1:])
    query_terms = tokenize(query_text)
    if not query_terms:
        print("No valid query terms found.")
        sys.exit(0)

    print(f"Query: '{query_text}'")
    print(f"Tokenized terms: {query_terms}")

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    row = session.execute(
        "SELECT total_docs, avg_dl FROM global_stats WHERE id = 1"
    ).one()
    if row is None:
        print("ERROR: global_stats not found. Run the indexer first.")
        sys.exit(1)

    N = row.total_docs
    avg_dl = row.avg_dl
    print(f"Corpus stats: N={N}, avg_dl={avg_dl:.2f}")

    posting_data = []
    query_term_set = set(query_terms)
    prepared = session.prepare(
        "SELECT term, doc_id, doc_title, tf, df, dl "
        "FROM inverted_index WHERE term = ?"
    )
    for term in query_term_set:
        rows = session.execute(prepared, (term,))
        for r in rows:
            posting_data.append((r.term, r.doc_id, r.doc_title, r.tf, r.df, r.dl))

    session.shutdown()
    cluster.shutdown()

    if not posting_data:
        print("No documents found for the given query terms.")
        sys.exit(0)

    print(f"Retrieved {len(posting_data)} posting entries")

    spark = SparkSession.builder \
        .appName("BM25 Search") \
        .getOrCreate()
    sc = spark.sparkContext

    N_bc = sc.broadcast(N)
    avg_dl_bc = sc.broadcast(avg_dl)

    postings_rdd = sc.parallelize(posting_data)

    def compute_bm25(record):
        term, doc_id, doc_title, tf, df, dl = record
        n = N_bc.value
        a = avg_dl_bc.value
        idf = math.log(n / df) if df > 0 else 0.0
        tf_component = ((K1 + 1) * tf) / (K1 * ((1 - B) + B * (dl / a)) + tf)
        score = idf * tf_component
        return ((doc_id, doc_title), score)

    scores_rdd = postings_rdd \
        .map(compute_bm25) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)

    top10 = scores_rdd.take(10)

    print(f"Top 10 results for query: '{query_text}'")
    for rank, ((doc_id, doc_title), score) in enumerate(top10, 1):
        print(f"  {rank:>2}. [Doc {doc_id}] {doc_title}  (score: {score:.4f})")

    spark.stop()


if __name__ == "__main__":
    main()
