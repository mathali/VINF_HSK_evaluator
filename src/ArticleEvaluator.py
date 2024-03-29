import jieba.posseg as pseg
import jieba
import multiprocessing as mp
from functools import partial

import pandas as pd


def evaluate_articles(articles,
                      hsk_dict,
                      output_file='../output/full_sample/evaluated_articles_fixed.csv',
                      eval=False):
    """
    Controller of the evaluation process
    :param articles: Preloaded list of json dictionaries containing articles + metadata
    :param hsk_dict: Dictionary containing official HSK ranking for each word
    :param output_file: Path to file for storing results
    :param eval: Flag to show if we are working with hskreading.com data
    """
    article_difficulties = {
        'HSK1': 0,
        'HSK2': 0,
        'HSK3': 0,
        'HSK4': 0,
        'HSK5': 0,
        'HSK6': 0,
    }

    # Determine output format
    if not eval:
        with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
            f.write('id\tHSK_level\tTime\tSource\tTitle\tContent\n')
    else:
        with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
            f.write('id\tLabeled Level\tEvaluated Level\n')

    hsk_level_dict = partial(__get_hsk_level, hsk_dict=hsk_dict)

    # Determines HSK level of each article
    # Serial processing if data frame, parallel processing if json list
    levels = []
    if isinstance(articles, pd.DataFrame):
        with open(output_file, 'a', newline='\n', encoding='utf-8') as f:
            for ind, article in articles.iterrows():
                current_level, article = __get_hsk_level(article, hsk_dict)
                levels.append(current_level)
                if eval:
                    f.write(f"{article['id']}\t{article['HSK_level']}\t{current_level}\n")
    else:
        pool = mp.Pool(mp.cpu_count())
        with open(output_file, 'a', newline='\n', encoding='utf-8') as f:
            for current_level, article in pool.map(hsk_level_dict, articles, chunksize=50):
                levels.append(current_level)
                if current_level != -1:
                    f.write(f"{article['news_id']}\t{current_level}\t{article['time']}\t{article['source']}\t{article['title']}\t{article['content']}\n")

    # Articles are discarded if they don't contain enough HSK graded words
    discarded_articles = 0

    # Determine distribution of levels
    for level in levels:
        if level != -1:
            article_difficulties['HSK' + str(level)] += 1
        else:
            discarded_articles += 1

    return article_difficulties, discarded_articles


def __get_hsk_level(article, hsk_dict):
    """
    Determines HSK level based on vocabulary and grammar (WIP)
    :param article: Article being evaluated
    :param hsk_dict: Dictionary of HSK graded words
    :return: HSK level of article, article in question
    """
    if isinstance(article, tuple):
        article = article[1]

    # Use jieba to segment article and remove useless terms - x (punctuation marks etc.), eng ( using latin alphabet),
    # m ( numbers )
    seg_list = pseg.cut(article['content'])
    seg_list = [x for x in seg_list if x.flag not in ['x', 'eng', 'm']]

    # Keep track of words from each level for final evaluation
    level_count = {
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
    }

    # Determine vocabulary and grammar level
    __word_evaluation(seg_list, level_count, hsk_dict)
    # __grammar_evaluation(seg_list, grammar_rules)

    current_level = 1
    current_count = level_count[current_level]
    total_level = sum(level_count.values())

    if total_level == 0:
        return -1, article

    # If article contains at least 80% of vocabulary from a current HSK level or lower, graded it as current HSK level
    # Based on 80-20 known-unknown rule for optimal language acquisition
    ratio = current_count / total_level
    while ratio < 0.8:
        current_level += 1
        current_count += level_count[current_level]
        ratio = current_count / total_level

    article['content'] = article['content'].replace('\t', ' ').replace('\n', '')

    return current_level, article


def __word_evaluation(seg_list, level_count, hsk_dict):
    """
    Dictionary lookup for each word to retrieve its HSK level
    :param seg_list: Segmented article
    :param level_count: Dictionary counting words for each level
    :param hsk_dict: Reference dictionary for lookups
    :return:
    """
    for s in seg_list:
        try:
            level_count[hsk_dict[s.word]] += 1
        except KeyError:
            continue


def __grammar_evaluation():
    pass
