import jieba.posseg as pseg
import jieba
import multiprocessing as mp
from functools import partial

import pandas as pd


def evaluate_articles(articles, hsk_dict, output_file='../output/evaluated_articles.csv', eval=False):
    # TODO : method to generate / retrieve list of grammar rules?
    article_difficulties = {
        'HSK1': 0,
        'HSK2': 0,
        'HSK3': 0,
        'HSK4': 0,
        'HSK5': 0,
        'HSK6': 0,
    }

    pool = mp.Pool(mp.cpu_count())

    if not eval:
        with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
            f.write('id,HSK_level,Time,Source,Title,Content\n')
    else:
        with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
            f.write('id,Labeled Level,Evaluated Level\n')

    hsk_level_dict = partial(__get_hsk_level, hsk_dict=hsk_dict, output_file=output_file, eval=eval)

    if isinstance(articles, pd.DataFrame):
        levels = pool.map(hsk_level_dict, articles.iterrows())
    else:
        levels = pool.map(hsk_level_dict, articles)

    # if not eval:
    #     with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
    #         f.write('id,HSK_level,Time,Source,Title,Content\n')
    #     pool = mp.Pool(mp.cpu_count())
    #     hsk_level_dict = partial(__get_hsk_level, hsk_dict=hsk_dict, output_file=output_file, eval=eval)
    #     levels = pool.map(hsk_level_dict, articles)
    # else:
    #     with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
    #         f.write('id,Labeled Level,Evaluated Level\n')
    #     for article in articles:
    #         levels = __get_hsk_level(article, hsk_dict, output_file, True)

    discarded_articles = 0

    for level in levels:
        if level != -1:
            article_difficulties['HSK' + str(level)] += 1
        else:
            discarded_articles += 1

    return article_difficulties, discarded_articles


def __get_hsk_level(article, hsk_dict, output_file, eval=False):
    with open(output_file, 'a', newline='\n', encoding='utf-8') as f:
        if isinstance(article, tuple):
            article = article[1]
        seg_list = pseg.cut(article['content'])

        seg_list = [x for x in seg_list if x.flag not in ['x', 'eng']]
        level_count = {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
        }

        __word_evaluation(seg_list, level_count, hsk_dict)
        # __grammar_evaluation(seg_list, grammar_rules)

        current_level = 1
        current_count = level_count[current_level]
        total_level = sum(level_count.values())

        if total_level == 0:
            return -1

        ratio = current_count / total_level
        while ratio < 0.8:
            current_level += 1
            current_count += level_count[current_level]
            ratio = current_count / total_level

        if not eval:
            f.write(f"{article['news_id']},{current_level},{article['time']},{article['source']},{article['title']},{article['content']}\n")
        else:
            f.write(f"{article['id']},{article['HSK_level']},{current_level}\n")

    return current_level


def __word_evaluation(seg_list, level_count, hsk_dict):
    for s in seg_list:
        try:
            level_count[hsk_dict[s.word]] += 1
        except KeyError:
            continue


def __grammar_evaluation():
    pass
