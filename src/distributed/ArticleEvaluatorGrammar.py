import ast
import time
import jieba.posseg as pseg
from pyspark.sql.functions import col
import os
import re
import pandas as pd
import json

import utils


def evaluate(levels, level_count=None):
    if not level_count:
        # Keeping track of words per each HSK level
        level_count = {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
        }

        # Analyze each word segmented by jieba
        for level in levels:
            if not level == -1:
                level_count[level] += 1

    current_level = 1
    current_count = level_count[current_level]
    total_level = sum(level_count.values())

    # Some articles in the dataset are weird and don't actually contain chinese text
    if total_level == 0:
        return -1

    # Calculate HSK difficulty based on the 80:20 rule
    # E.g. if HSK1 + HSK2 + HSK3 characters represent 80% of the text, the article is graded as HSK3
    ratio = current_count / total_level
    while ratio < 0.8:
        current_level += 1
        current_count += level_count[current_level]
        ratio = current_count / total_level

    return current_level


def map_levels(word, hsk_map):
    """
    Dictionary lookup for each word to retrieve its HSK level
    :param seg_list: Segmented article
    :param level_count: Dictionary counting words for each level
    :param hsk_dict: Reference dictionary for lookups
    :return:
    """
    try:
        return hsk_map.value[word]
    except KeyError:
        return -1


def map_grammar(content, grammar_map):
    level_count = {
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
    }
    text = ''.join([x.word for x in content])

    for row in grammar_map.value:
        row = pd.DataFrame(row)
        row.iloc[1] = row.iloc[1].apply(ast.literal_eval)
        row.iloc[2] = row.iloc[2].apply(ast.literal_eval)
        row = row.values
        char_pattern = ''.join(row[1][0])
        pos_pattern = ''.join(row[2][0])

        c_p = re.compile(char_pattern)
        p_p = re.compile(pos_pattern)

        # Find a position with characters that correspond to the current grammar rule
        for match in re.finditer(c_p,text):
            tags = ''.join([x.flag for x in content[match.start():match.end()]])
            # Determine if the PoS tags at the match location fulfill the second part of the rule
            pos_match = re.search(p_p, tags)
            if pos_match:
                level_count[row[0][0]] = level_count[row[0][0]] + 1

    return evaluate(None, level_count)



def evaluation_grammar():
    """
    Evaluation requires a separate process due to the dataset structure being different

    :return:
    """
    spark = utils.setup_spark()
    articles = utils.get_articles(spark, 'eval')
    grammar_map = utils.get_grammar_mapping(spark)
    hsk_map = utils.get_hsk_dict(spark)

    # Perform all the steps necessary for article evaluation
    # 1. Process the text using jieba
    # 2. Determine the level of each character
    # 3. Determine the level of the whole article
    out = articles.rdd.repartition(100) \
        .map(lambda article: (article['id'],
                              [x.word for x in pseg.cut(article['content']) if x.flag not in ['x', 'eng', 'm']],
                              map_grammar([x for x in pseg.cut(article['content'])], grammar_map),
                              article['HSK_level'],
                              article['URL'],
                              article['Title_EN'],
                              article['Title_ZH'],
                              article['content'])) \
        .map(lambda words: (words[0],
                            [map_levels(word, hsk_map) for word in words[1]],
                            words[2],
                            words[3],
                            words[4],
                            words[5],
                            words[6],
                            words[7])) \
        .map(lambda levels: (levels[0],
                             evaluate(levels[1]),
                             levels[2],
                             levels[3],
                             levels[4],
                             levels[5],
                             levels[6],
                             levels[7]))

    out = out.toDF().select(col('_1').alias('id'), col('_2').alias('Evaluated Level'), col('_3').alias('Grammar Level'),
                            col('_4').alias('Labeled Level'), col('_5').alias('URL'), col('_6').alias('Title_EN'),
                            col('_7').alias('Title_ZH'), col('_8').alias('content'))

    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    out.toPandas().to_csv(f'../../output/full_sample/distributed/evaluated_eval_grammar_partitions.csv', index=False,
                          sep='\t')


def main_grammar(mode='valid'):
    """
    Core of the functionality for the 'news2016zh' dataset

    :param mode:  determines which part of the dataset is loaded
    :return:
    """
    spark = utils.setup_spark()
    articles = utils.get_articles(spark, mode)
    grammar_map = utils.get_grammar_mapping(spark)
    hsk_map = utils.get_hsk_dict(spark)

    if mode == 'train':
        # Normally 2.5 mil, but limited for experimenting how the process works on with a lot of data without
        # having to wait 6h
        articles = articles.limit(150000)
    elif mode == 'demo':
        articles = articles.limit(1000)

    # Drastically increase the number of partitions for the full dataset so it can actually fit into memory
    if mode == 'full':
        n_partitions = 2000
    else:
        n_partitions = 100

    # Perform all the steps necessary for article evaluation
    # 1. Process the text using jieba
    # 2. Determine the level of each character
    # 3. Determine the level of the whole article
    out = articles.rdd.repartition(n_partitions)\
                      .map(lambda article: (article['news_id'],
                                            [x.word for x in pseg.cut(article['content']) if x.flag not in ['x', 'eng', 'm']],
                                            map_grammar([x for x in pseg.cut(article['content'])], grammar_map),
                                            article['time'],
                                            article['source'],
                                            article['title'],
                                            article['keywords'],
                                            article['desc'])) \
                      .map(lambda words: (words[0],
                                          [map_levels(word, hsk_map) for word in words[1]],
                                          words[2],
                                          words[3],
                                          words[4],
                                          words[5],
                                          words[6],
                                          words[7])) \
                      .map(lambda levels: (levels[0],
                                           evaluate(levels[1]),
                                           levels[2],
                                           levels[3],
                                           levels[4],
                                           levels[5],
                                           levels[6],
                                           levels[7]))

    out = out.toDF().select(col('_1').alias('news_id'), col('_2').alias('level'), col('_3').alias('grammar_level'),
                            col('_4').alias('time'), col('_5').alias('source'), col('_6').alias('title'),
                            col('_7').alias('keywords'), col('_8').alias('desc'))

    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    out.toPandas().to_csv(f'../../output/full_sample/distributed/evaluated_{mode}_grammar_partitions.csv', index=False, sep='\t')


def run():
    mode = input('Specify mode (demo/train/valid/full): ')
    start = time.time()
    spark = utils.setup_spark()
    main_grammar(mode)
    end = time.time()
    print(f'Duration: {(end-start)/60} min')


if __name__ == '__main__':
    run()
