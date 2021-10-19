import jieba.posseg as pseg
import json
import pandas as pd
import time
import numpy as np
import multiprocessing as mp
from functools import partial
from collections import Counter


def __get_tf(terms):
    """
    Calculates Term frequency with a simple ration of number of occurrences / length of phrase for each word
    :param terms: Segmented phrase
    :return: Term frequency for each word
    """
    tf_dict = {}
    for t in terms:
        if t in tf_dict:
            tf_dict[t] += 1
        else:
            tf_dict[t] = 1

    for key, value in tf_dict.items():
        tf_dict[key] = value / len(terms)

    return tf_dict


def __get_idf(index, N):
    """
    Calculates log inverted document frequency. Based on number of keys per entry in inverted index.
    :param index: Inverted index word: docID
    :return:
    """
    idf = {}
    for key, value in index.items():
        df = len(value.keys())
        idf[key] = np.log(N/df)

    return idf


def create_index_tf_idf(mode='showcase'):
    """
    Generate inverted index file with TF-IDF weights
    """
    index = {}

    # Load articles processed by ArticleEvaluator.py
    articles = pd.read_csv(
        open(f'../output/{mode}/evaluated_articles_fixed.csv', errors='replace', encoding='utf8'),
        delimiter='\t',
        on_bad_lines='warn')

    start = time.time()
    for ind, a in articles.iterrows():
        if ind % 5000 == 0:
            print(f'Processed {ind} articles')

        if isinstance(a['Content'], float):
            continue

        # Segment each article using jieba
        terms = pseg.cut(a['Content'])
        terms = [x.word for x in terms if x.flag not in ['x', 'eng', 'm']]

        # Calculate Term frequencies for each term in article
        tf = __get_tf(terms)

        # Store TF in a proto-index dictionary
        for key, value in tf.items():
            if key in index:
                index[key][ind] = value
            else:
                index[key] = {ind: value}

    # Calculate IDF for each term in proto-index
    idf = __get_idf(index, len(articles))

    # Modify index to store proper TF-IDF weight for each term-DocID pairing
    for k1, v1 in index.items():
        for k2, v2 in v1.items():
            v1[k2] = round(v2 * idf[k1], 3)

    end = time.time()
    print(f'Duration: {(end - start) / 60} min')
    # Store finished inverted-index
    with open(f'../output/demonstration/{mode}/inverted_index_tfidf.json', 'w') as f:
        json.dump(index, f)
    # Store IDF for each term to be used during full-text search
    with open(f'../output/demonstration/{mode}/idf.json', 'w') as f:
        json.dump(idf, f)


def create_index(mode='showcase'):
    """
    Creates simple inverted-index based on word counts in articles (without weighing).
    """
    index = {}

    # Load articles processed by ArticleEvaluator.py
    articles = pd.read_csv(open(f'../output/demonstration/{mode}/evaluated_articles_fixed.csv', errors='replace', encoding='utf8'),
                           delimiter='\t',
                           on_bad_lines='warn')

    start = time.time()
    for ind, a in articles.iterrows():
        if ind % 5000 == 0:
            print(f'Processed {ind} articles')

        if isinstance(a['Content'], float):
            continue

        # Segment each article using jieba
        terms = pseg.cut(a['Content'])
        terms = [x.word for x in terms if x.flag not in ['x', 'eng', 'm']]

        # Matches each term to each document that contains it, store number of occurrences for each document
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
    # Store inverted-index for later use
    with open(f'../output/demonstration/{mode}/inverted_index_fixed.json', 'w') as f:
        json.dump(index, f)


def __cosine_similarity(article, seg_phrase, output, idf=None, idf_flag=False):
    """
    Calculate cosine similarity to determine the closeness of article to search query
    :param article: Body of text to be evaluated
    :param seg_phrase: Segmented phrase we are looking for in an article
    :param output: Used by multiprocessing. Dict to store results.
    :param idf: Dictionary of precalculated IDF. Only used if idf_flag == True
    :param idf_flag: Determines if similarity calculations are based on TF-IDF
    :return:
    """
    if isinstance(article, tuple):
        article = article[1]

    seg_article = pseg.cut(article['Content'])
    seg_article = [x.word for x in seg_article if x.flag not in ['x', 'eng', 'm']]

    # Create vectors used in similarity calculation. Either TF-IDF or count based.
    if idf_flag:
        d1 = __get_tf(seg_phrase)
        for key, value in d1.items():
            d1[key] = value * idf[key]

        d2 = __get_tf(seg_article)
        for key, value in d2.items():
            d2[key] = value * idf[key]
    else:
        d1 = Counter(seg_phrase)
        d2 = Counter(seg_article)

    # Calculate dot product of query and article vectors. KeyError simulates adding 0 - term not found in article
    dot_product = 0
    for x in seg_phrase:
        try:
            dot_product += d1[x] * d2[x]
        except KeyError:
            continue

    # Prepare denominator variables for similarity equation
    sum1 = sum([d1[x] ** 2 for x in list(d1.keys())])
    sum2 = sum([d2[x] ** 2 for x in list(d2.keys())])

    abs_vals = np.sqrt(sum1) * np.sqrt(sum2)

    # Similarity value is either returned (serial) or stored in a shared dict (parallel)
    if output is not None:
        output[article['id']] = dot_product/abs_vals
    else:
        return dot_product/abs_vals


def text_search(phrase='我你好', mode='and', size='sample', idf_flag=False):
    """
    Full-text search of provided articles.
    :param phrase: Search query. The basis for similarity calculations.
    :param mode: 'and'/'or' Determines if all terms in query must be included in an article
    :param size: 'sample' if we want to work with 1000 article version. For quicker demonstration
    :param idf_flag: Determines if we use count based or TF-IDF based index
    """

    idf = None

    # Load or required data into memory
    if size == 'sample':
        if idf_flag:
            index_file = '../output/showcase/inverted_index_tfidf.json'
        else:
            index_file = '../output/showcase/inverted_index_fixed.json'
        articles_file = '../output/showcase/evaluated_articles_fixed.csv'
        idf_file = '../output/showcase/idf.json'
    else:
        if idf_flag:
            index_file = '../output/full_sample/inverted_index_tfidf.json'
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

    # Prepare phrase by segmenting it in jieba and removing useless terms.
    seg_phrase = pseg.cut(phrase)
    seg_phrase = [x.word for x in seg_phrase if x.flag not in ['x', 'eng', 'm']]

    # Find get list of documents for each term in phrase
    doc_list = []
    for w in seg_phrase:
        try:
            doc_list.append([int(x) for x in index[w].keys()])
        except KeyError:
            print('Warning: Term not indexed')

    # Check to make sure submitted phrase makes sense
    if len(doc_list) == 0:
        print('Error: No matches found')
        return

    # Only keep documents that contain each term
    if mode == 'and':
        match = set(doc_list[0])
        if len(doc_list) > 0:
            for doc in doc_list[1:]:
                match = match & set(doc)
    # Keep all documents with at least one term
    else:
        match = set()
        for doc in doc_list:
            match.update(doc)

    no_matches = len(match)
    print(f'Number of possible matches: {no_matches}')

    # Serial processing is quicker if we are analyzing a small number of articles
    if no_matches < 200:
        doc_similarities = {}
        for doc in match:
            doc_similarities[doc] = __cosine_similarity(articles.iloc[doc], seg_phrase, None, idf, idf_flag)
    # Parallelize similarity calculations for large number of articles
    else:
        pool = mp.Pool(mp.cpu_count())
        manager = mp.Manager()
        doc_similarities = manager.dict()
        cosine_partial = partial(__cosine_similarity, seg_phrase=seg_phrase, output=doc_similarities, idf=idf, idf_flag=idf_flag)

        pool.map(cosine_partial, articles.iloc[list(match)].iterrows())

    # Select and print article with highest similarity
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
