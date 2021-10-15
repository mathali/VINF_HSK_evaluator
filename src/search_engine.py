import jieba.posseg as pseg
import json
import pandas as pd
import time
import numpy as np
import multiprocessing as mp
from functools import partial
from collections import Counter


def create_index():
    index = {}
    # TODO: Find source of field number warnings -> maybe \t in text
    articles = pd.read_csv(open('../output/showcase/evaluated_articles_1000.csv', errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    start = time.time()
    for ind, a in articles.iterrows():
        if ind % 100 == 0:
            print(f'Processed {ind} articles')

        if isinstance(a['Content'], float):
            continue
        terms = pseg.cut(a['Content'])
        terms = [x for x in terms if x.flag not in ['x', 'eng', 'm']]

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
    with open('../output/showcase/inverted_index.json', 'w') as f:
        json.dump(index, f)


def cosine_similarity(article, seg_phrase, output):
    if isinstance(article, tuple):
        article = article[1]
        seg_article = pseg.cut(article['Content'])
    else:
        seg_article = pseg.cut(article['Content'].values[0])
    seg_article = [x.word for x in seg_article if x.flag not in ['x', 'eng']]

    d1 = Counter(seg_phrase)
    d2 = Counter(seg_article)

    dot_product = sum([d1[x] * d2[x] for x in seg_phrase])

    sum1 = sum([d1[x] ** 2 for x in list(d1.keys())])
    sum2 = sum([d2[x] ** 2 for x in list(d2.keys())])

    abs_vals = np.sqrt(sum1) * np.sqrt(sum2)

    if output is not None:
        output[article['id']] = dot_product/abs_vals
    else:
        return dot_product/abs_vals



def text_search(phrase='我你好', mode='u'):
    with open('../output/full_sample/inverted_index.json', 'r') as f:
        index = json.load(f)

    articles = pd.read_csv(open('../output/full_sample/evaluated_articles_new.csv', errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    seg_phrase = pseg.cut(phrase)
    seg_phrase = [x.word for x in seg_phrase if x.flag not in ['x', 'eng']]

    doc_list = []
    for w in seg_phrase:
        try:
            doc_list.append(index[w].keys())
        except KeyError:
            print('Warning: Term not indexed')

    if len(doc_list) == 0:
        print('Error: No matches found')
        return

    if mode == 'i':
        match = set(doc_list[0])
        if len(doc_list) > 0:
            for doc in doc_list[1:]:
                match = match & set(doc)
    else:
        match = set()
        for doc in doc_list:
            match.update(doc)

    if len(match) < 200:
        doc_similarities = {}
        for doc in match:
            doc_similarities[doc] = cosine_similarity(articles[articles['id'] == doc], seg_phrase, None)
    else:
        pool = mp.Pool(mp.cpu_count())
        manager = mp.Manager()
        doc_similarities = manager.dict()
        cosine_partial = partial(cosine_similarity, seg_phrase=seg_phrase, output=doc_similarities)

        pool.map(cosine_partial, articles[articles['id'].isin(match)].iterrows())

    max_key = max(doc_similarities, key=doc_similarities.get)
    print(f'Highest cosine similarity: {doc_similarities[max_key]}')
    print('With article:')
    print(articles[articles['id'] == max_key]['Content'].values)
    # weird = [k for k,v in index.items() if len(v) <= 1]
    # print(len(weird))


if __name__ == '__main__':
    text_search(mode='i')