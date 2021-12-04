import os
import time
import lucene

from java.nio.file import Paths
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig)
from org.apache.lucene.store import MMapDirectory
from tqdm import tqdm


def main(mode):
    # Basic Lucene setup
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get(f'{mode}_index'))
    writerConfig = IndexWriterConfig(SmartChineseAnalyzer())    # Analyzer that works with chinese text

    # Delete any existing index with the same name
    writer = IndexWriter(fsDir, writerConfig)
    writer.deleteAll()
    # Make sure everything got deleted
    print(f"{writer.getPendingNumDocs()} docs found in index")

    # Basic index for short simple fields that shouldn't vary a lot
    t1 = FieldType()
    t1.setStored(True)
    t1.setIndexOptions(IndexOptions.DOCS)

    # More complex index for 'text-based' fields to accommodate more robust queries
    t2 = FieldType()
    t2.setStored(True)
    t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with open(f'../../output/full_sample/distributed/evaluated_{mode}_partitions.csv') as articles:
        head = False
        for ind, row in enumerate(tqdm(articles)):
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

            # Partial index commits for modes working with a large amount of data
            if ind % 50000 == 0:
                writer.commit()

    print(f"{writer.getPendingNumDocs()} docs found in index")
    writer.commit()
    writer.close()


def run():
    mode = input('Specify mode (demo/train/valid/full): ')
    start = time.time()
    main(mode)
    end = time.time()
    print(f'Duration: {(end-start)/60} min')


if __name__ == '__main__':
    run()
