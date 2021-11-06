import sys
import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import (IndexOptions, IndexWriter, DirectoryReader,
                                     IndexWriterConfig)
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.queryparser.classic import QueryParser
# from lucene import SimpleFSDirectory, IndexSearcher, Version, QueryParser

"""
PyLucene retriver simple example
"""

INDEXDIR = "./index"


def luceneRetriver(query):
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get('train_index'))
    lucene_analyzer = SmartChineseAnalyzer()

    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)

    my_query = QueryParser("desc", lucene_analyzer).parse(query)
    MAX = 10
    total_hits = searcher.search(my_query, MAX)
    print("Hits: ", total_hits.totalHits)

    for hit in total_hits.scoreDocs:
        print("Hit Score: ", hit.score, "Hit Doc:", hit.doc, "Hit String:", hit.toString())
        doc = searcher.doc(hit.doc)
        print(doc.get('desc'))
        #print(doc.get("level").encode("utf-8"))


luceneRetriver("个")
