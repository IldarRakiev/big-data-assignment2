import sys

current_term = None
postings = []


def flush(term, postings):
    df = len(postings)
    for doc_id, doc_title, tf, dl in postings:
        print(f"{term}\t{doc_id}\t{doc_title}\t{tf}\t{df}\t{dl}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 5:
        continue
    term, doc_id, doc_title, tf, dl = parts
    if term != current_term:
        if current_term is not None:
            flush(current_term, postings)
        current_term = term
        postings = []
    postings.append((doc_id, doc_title, tf, dl))

if current_term is not None:
    flush(current_term, postings)
