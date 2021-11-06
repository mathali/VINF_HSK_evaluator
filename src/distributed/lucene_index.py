import time

import lucene

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig)
from org.apache.lucene.store import MMapDirectory

env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
fsDir = MMapDirectory(Paths.get('train_index'))
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

start = time.time()
with open('../../output/full_sample/distributed/evaluated_train.csv') as articles:
    head = False
    for row in articles:
        if not head:
            fields = row.replace('\n', '').split('\t')
            #fields = ['news_id', 'level', 'time', 'source', 'title', 'keywords', 'desc']
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

end = time.time()

print(f"{writer.getPendingNumDocs()} docs found in index")
writer.commit()
writer.close()
print(f'Duration: {(end-start)/60} min')