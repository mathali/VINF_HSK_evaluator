import jieba.posseg as pseg
import json
import pandas as pd
import time
import numpy as np
import multiprocessing as mp
from functools import partial
from collections import Counter

def get_tf(terms):
    tf_dict = {}
    for t in terms:
        if t in tf_dict:
            tf_dict[t] += 1
        else:
            tf_dict[t] = 1

    for key,value in tf_dict.items():
        tf_dict[key] = value / len(terms)

    return tf_dict

def get_idf(index):
    idf = {}
    for key, value in index.items():
        df = len(value.keys())
        idf[key] = np.log(len(index.keys())/df)

    return idf

def create_index_tf_idf():
    index = {}
    articles = pd.read_csv(
        open('../output/showcase/evaluated_articles_fixed.csv', errors='replace', encoding='utf8'),
        delimiter='\t',
        on_bad_lines='warn')

    start = time.time()
    for ind, a in articles.iterrows():
        if ind % 5000 == 0:
            print(f'Processed {ind} articles')

        if isinstance(a['Content'], float):
            continue
        terms = pseg.cut(a['Content'])
        terms = [x.word for x in terms if x.flag not in ['x', 'eng', 'm']]

        tf = get_tf(terms)

        for key, value in tf.items():
            if key in index:
                index[key][ind] = value
            else:
                index[key] = {ind: value}

    idf = get_idf(index)

    for k1, v1 in index.items():
        for k2, v2 in v1.items():
            v1[k2] = round(v2 * idf[k1], 2)

    end = time.time()
    print(f'Duration: {(end - start) / 60} min')
    with open('../output/showcase/inverted_index_tfidf.json', 'w') as f:
        json.dump(index, f)
    with open('../output/showcase/idf.json', 'w') as f:
        json.dump(idf, f)

def create_index():
    index = {}
    articles = pd.read_csv(open('../output/full_sample/evaluated_articles_fixed.csv', errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    start = time.time()
    for ind, a in articles.iterrows():
        if ind % 5000 == 0:
            print(f'Processed {ind} articles')

        if isinstance(a['Content'], float):
            continue
        terms = pseg.cut(a['Content'])
        terms = [x.word for x in terms if x.flag not in ['x', 'eng', 'm']]

        for t in terms:
            if t in index:
                if ind not in index[t]:
                    index[t][ind] = 1
                else:
                    index[t][ind] += 1
            else:
                index[t] = {ind: 1}

    end = time.time()
    print(f'Duration: {(end-start)/60} min')
    with open('../output/full_sample/inverted_index_fixed.json', 'w') as f:
        json.dump(index, f)


def cosine_similarity(article, seg_phrase, output, idf=None, idf_flag=False):
    if isinstance(article, tuple):
        article = article[1]

    seg_article = pseg.cut(article['Content'])
    seg_article = [x.word for x in seg_article if x.flag not in ['x', 'eng', 'm']]

    if idf_flag:
        d1 = get_tf(seg_phrase)
        for key, value in d1.items():
            d1[key] = value * idf[key]

        d2 = get_tf(seg_article)
        for key, value in d2.items():
            d2[key] = value * idf[key]
    else:
        d1 = Counter(seg_phrase)
        d2 = Counter(seg_article)

    dot_product = 0
    for x in seg_phrase:
        try:
            dot_product += d1[x] * d2[x]
        except KeyError:
            continue

    sum1 = sum([d1[x] ** 2 for x in list(d1.keys())])
    sum2 = sum([d2[x] ** 2 for x in list(d2.keys())])

    abs_vals = np.sqrt(sum1) * np.sqrt(sum2)

    if output is not None:
        output[article['id']] = dot_product/abs_vals
    else:
        return dot_product/abs_vals


def text_search(phrase='我你好', mode='and', size='sample', idf_flag=False):
    idf = None
    if size == 'sample':
        if idf_flag:
            index_file = '../output/showcase/inverted_index_tfidf.json'
        else:
            index_file = '../output/showcase/inverted_index_fixed.json'
        articles_file = '../output/showcase/evaluated_articles_fixed.csv'
        idf_file = '../output/showcase/idf.json'
    else:
        index_file = '../output/full_sample/inverted_index_fixed.json'
        articles_file = '../output/full_sample/evaluated_articles_fixed.csv'
        idf_file = '../output/full_sample/idf.json'

    with open(index_file, 'r') as f:
        index = json.load(f)

    if idf_flag:
        with open(idf_file, 'r') as f:
            idf = json.load(f)

    articles = pd.read_csv(open(articles_file, errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    seg_phrase = pseg.cut(phrase)
    seg_phrase = [x.word for x in seg_phrase if x.flag not in ['x', 'eng', 'm']]

    doc_list = []
    for w in seg_phrase:
        try:
            doc_list.append([int(x) for x in index[w].keys()])
        except KeyError:
            print('Warning: Term not indexed')

    if len(doc_list) == 0:
        print('Error: No matches found')
        return

    if mode == 'and':
        match = set(doc_list[0])
        if len(doc_list) > 0:
            for doc in doc_list[1:]:
                match = match & set(doc)
    else:
        match = set()
        for doc in doc_list:
            match.update(doc)

    no_matches = len(match)
    print(f'Number of possible matches: {no_matches}')

    if no_matches < 200:
        doc_similarities = {}
        for doc in match:
            doc_similarities[doc] = cosine_similarity(articles.iloc[doc], seg_phrase, None, idf, idf_flag)
    else:
        pool = mp.Pool(mp.cpu_count())
        manager = mp.Manager()
        doc_similarities = manager.dict()
        cosine_partial = partial(cosine_similarity, seg_phrase=seg_phrase, output=doc_similarities, idf=idf, idf_flag=idf_flag)

        pool.map(cosine_partial, articles.iloc[list(match)].iterrows())

    max_key = max(doc_similarities, key=doc_similarities.get)
    print(f'Highest cosine similarity: {doc_similarities[max_key]}')
    print('With article:')
    if no_matches >= 200:
        print(articles[articles['id'] == max_key]['Content'].values[0])
    else:
        print(articles.iloc[max_key]['Content'])


if __name__ == '__main__':
    start = time.time()
    text_search(mode='or', size='sample', idf_flag=False)
    end = time.time()
    print(f'Search duration: {(end-start)/60} min')
    # create_index_tf_idf()