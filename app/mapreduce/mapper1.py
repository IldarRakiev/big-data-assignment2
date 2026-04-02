import sys
import re
from collections import Counter

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue
    doc_id, doc_title, doc_text = parts
    tokens = re.findall(r"[a-z0-9]+", doc_text.lower())
    if not tokens:
        continue
    dl = len(tokens)
    term_counts = Counter(tokens)
    for term, tf in term_counts.items():
        print(f"{term}\t{doc_id}\t{doc_title}\t{tf}\t{dl}")
