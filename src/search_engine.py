import jieba.posseg as pseg
import json
import pandas as pd
import time
import sys


def create_index():
    index = {}
    # TODO: Find source of field number warnings -> maybe \t in text
    articles = pd.read_csv(open('../output/evaluated_articles_new.csv', errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    start = time.time()
    for ind, a in articles.iterrows():
        if ind % 5000 == 0:
            print(f'Processed {ind} articles')
        try:
            length = len(a['Content'])
            terms = pseg.cut(a['Content'])
        except (AttributeError, TypeError):
            continue
        terms = [x for x in terms if x.flag not in ['x', 'eng']]

        for t in terms:
            if t.word in index:
                if a['id'] not in index[t.word]:
                    index[t.word][a['id']] = 1
                else:
                    index[t.word][a['id']] += 1
            else:
                index[t.word] = {a['id']: 1}

    end = time.time()
    print(f'Duration: {(end-start)/60} min')
    with open('../output/inverted_index.json', 'w') as f:
        json.dump(index, f)


if __name__ == '__main__':
    create_index()