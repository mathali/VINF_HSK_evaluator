import time

import lucene

from java.nio.file import Paths
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig)
from org.apache.lucene.store import MMapDirectory

def main(mode):
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get(f'{mode}_index'))
    writerConfig = IndexWriterConfig(SmartChineseAnalyzer())
    writer = IndexWriter(fsDir, writerConfig)
    writer.deleteAll()
    print(f"{writer.getPendingNumDocs()} docs found in index")


    # Define field type
    t1 = FieldType()
    t1.setStored(True)
    t1.setIndexOptions(IndexOptions.DOCS)

    t2 = FieldType()
    t2.setStored(True)
    t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

    with open(f'../../output/full_sample/distributed/evaluated_{mode}_partitions.csv') as articles:
        head = False
        for ind, row in enumerate(articles):
            if not head:
                fields = row.replace('\n', '').split('\t')
                head = True
            else:
                doc = Document()
                row = row.replace('\n', '').split('\t')
                for field in range(len(fields)):
                    if fields[field] in ['news_id', 'time', 'source', 'title']:
                        t = t1
                    else:
                        t = t2
                    doc.add(Field(fields[field], row[field], t))
                writer.addDocument(doc)

            if ind % 50000 == 0:
                print(f'Indexed {ind} articles')
                writer.commit()

    print(f"{writer.getPendingNumDocs()} docs found in index")
    writer.commit()
    writer.close()

if __name__ == '__main__':
    start = time.time()
    main('full')
    end = time.time()
    print(f'Duration: {(end-start)/60} min')