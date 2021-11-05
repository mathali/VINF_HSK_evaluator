import lucene

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig)
from org.apache.lucene.store import MMapDirectory

env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
fsDir = MMapDirectory(Paths.get('index'))
writerConfig = IndexWriterConfig(SmartChineseAnalyzer())
writer = IndexWriter(fsDir, writerConfig)
print(f"{writer.getPendingNumDocs()} docs found in index")


# Define field type
t1 = FieldType()
t1.setStored(True)
t1.setIndexOptions(IndexOptions.DOCS)

t2 = FieldType()
t2.setStored(False)
t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

with open('../../output/full_sample/distributed/test.csv') as articles:
    head = False
    for row in articles:
        if not head:
            #fields = row.replace('\n', '').split('\t')
            fields = ['news_id', 'level', 'time', 'source', 'title', 'keywords', 'desc']
            head = True
        else:
            doc = Document()
            for field in range(len(fields)):
                if fields[field] in ['news_id', 'time', 'source', 'title']:
                    t = t1
                else:
                    t = t2
                doc.add(Field(fields[field], row[field], t))
            writer.addDocument(doc)
            #print(row.replace('\n', '').split('\t'))

# Add a document
# doc = Document()
# doc.add(Field('id', '418129481', t1))
# doc.add(Field('title', 'Lucene in Action', t1))
# doc.add(Field('text', 'Hello Lucene, This is the first document', t2))
# writer.addDocument(doc)
print(f"{writer.getPendingNumDocs()} docs found in index")
writer.commit()