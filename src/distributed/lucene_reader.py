import time

import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import (IndexOptions, IndexWriter, DirectoryReader,
                                     IndexWriterConfig)
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.queryparser.classic import QueryParser


def search(mode, query, field):
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get(f'{mode}_index'))
    lucene_analyzer = SmartChineseAnalyzer()

    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)

    my_query = QueryParser(field, lucene_analyzer).parse(query)
    MAX = 10
    total_hits = searcher.search(my_query, MAX)
    print("Hits: ", total_hits.totalHits)

    for hit in total_hits.scoreDocs:
        print("Hit Score: ", hit.score, "Hit Doc:", hit.doc, "Hit String:", hit.toString())
        doc = searcher.doc(hit.doc)
        print(f'news_id: {doc["news_id"]}, level: {doc["level"]}, time: {doc["time"]}, '
              f'source: {doc["source"]}, title: {doc["title"]}, keywords: {doc["keywords"]}, desc: {doc["desc"]}')


def run():
    mode = input('Specify mode (demo/train/valid/full): ')
    field = input('Default Field (news_id/level/time/source/title/keywords/desc): ')
    query = input('Valid Lucene search query: ')
    if field not in ['news_id', 'level', 'time', 'source', 'title', 'keywords', 'desc']:
        print('Invalid field')
    else:
        start = time.time()
        search(mode, query, field)
        end = time.time()
        print(f'Lookup time: {end-start} s ')


if __name__ == '__main__':
    run()
