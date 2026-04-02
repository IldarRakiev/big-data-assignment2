#!/bin/bash
service ssh restart
bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
.venv/bin/venv-pack -o .venv.tar.gz

bash prepare_data.sh

bash index.sh

# Save all important results to a file for easy viewing
RESULTS=/app/results.txt
echo "" > $RESULTS

echo "========================================" >> $RESULTS
echo "  DOCUMENTS IN HDFS /data" >> $RESULTS
echo "========================================" >> $RESULTS
hdfs dfs -ls /data >> $RESULTS 2>&1

echo "" >> $RESULTS
echo "========================================" >> $RESULTS
echo "  INDEX SAMPLE FROM HDFS /indexer/index" >> $RESULTS
echo "========================================" >> $RESULTS
hdfs dfs -cat /indexer/index/part-* 2>/dev/null | head -30 >> $RESULTS

echo "" >> $RESULTS
echo "========================================" >> $RESULTS
echo "  CASSANDRA: inverted_index (20 rows)" >> $RESULTS
echo "========================================" >> $RESULTS
python3 -c "
from cassandra.cluster import Cluster
s = Cluster(['cassandra-server']).connect('search_engine')
print('{:<15} {:<12} {:<40} {:>4} {:>4} {:>6}'.format('TERM','DOC_ID','DOC_TITLE','TF','DF','DL'))
print('-'*90)
for r in s.execute('SELECT * FROM inverted_index LIMIT 20'):
    print('{:<15} {:<12} {:<40} {:>4} {:>4} {:>6}'.format(r.term, r.doc_id, r.doc_title[:38], r.tf, r.df, r.dl))
" >> $RESULTS

echo "" >> $RESULTS
echo "========================================" >> $RESULTS
echo "  CASSANDRA: doc_stats (15 rows)" >> $RESULTS
echo "========================================" >> $RESULTS
python3 -c "
from cassandra.cluster import Cluster
s = Cluster(['cassandra-server']).connect('search_engine')
print('{:<12} {:<45} {:>6}'.format('DOC_ID','DOC_TITLE','DL'))
print('-'*65)
for r in s.execute('SELECT * FROM doc_stats LIMIT 15'):
    print('{:<12} {:<45} {:>6}'.format(r.doc_id, r.doc_title[:43], r.dl))
" >> $RESULTS

echo "" >> $RESULTS
echo "========================================" >> $RESULTS
echo "  CASSANDRA: global_stats" >> $RESULTS
echo "========================================" >> $RESULTS
python3 -c "
from cassandra.cluster import Cluster
s = Cluster(['cassandra-server']).connect('search_engine')
for r in s.execute('SELECT * FROM global_stats'):
    print(f'total_docs={r.total_docs}, avg_dl={r.avg_dl:.2f}')
" >> $RESULTS

echo "" >> $RESULTS
echo "Results saved to $RESULTS"
cat $RESULTS

bash search.sh "history of art" 2>&1 | tee -a $RESULTS
bash search.sh "world war" 2>&1 | tee -a $RESULTS
bash search.sh "music album rock" 2>&1 | tee -a $RESULTS

echo ""
echo "========================================"
echo "  ALL DONE - results saved to $RESULTS"
echo "========================================"
