import jieba.posseg as pseg
import json
import pandas as pd
import time
import numpy as np
from collections import Counter


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


def cosine_similarity(seg_phrase, article):

    seg_article = pseg.cut(article['Content'].values[0])
    seg_article = [x.word for x in seg_article if x.flag not in ['x', 'eng']]

    d1 = Counter(seg_phrase)
    d2 = Counter(seg_article)

    dot_product = sum([d1[x] * d2[x] for x in seg_phrase])

    sum1 = sum([d1[x] ** 2 for x in list(d1.keys())])
    sum2 = sum([d2[x] ** 2 for x in list(d2.keys())])

    abs_vals = np.sqrt(sum1) * np.sqrt(sum2)

    return dot_product/abs_vals



def text_search(phrase='我你好'):
    with open('../output/inverted_index.json', 'r') as f:
        index = json.load(f)

    articles = pd.read_csv(open('../output/evaluated_articles_new.csv', errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    seg_phrase = pseg.cut(phrase)
    seg_phrase = [x.word for x in seg_phrase if x.flag not in ['x', 'eng']]

    doc_list = []
    for w in seg_phrase:
        doc_list.append(index[w].keys())

    union = set()
    for doc in doc_list:
        union.update(doc)

    doc_similarities = {}
    for doc in union:
        doc_similarities[doc] = cosine_similarity(seg_phrase, articles[articles['id']==doc])


    # print(max(doc_similarities))
    # weird = [k for k,v in index.items() if len(v) <= 1]
    # print(len(weird))


if __name__ == '__main__':
    text_search()