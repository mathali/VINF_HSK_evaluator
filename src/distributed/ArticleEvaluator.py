import time
import jieba.posseg as pseg
from pyspark.sql.functions import col

import utils


def evaluate(levels):
    level_count = {
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
    }

    for level in levels:
        if not level == -1:
            level_count[level] += 1

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

    return current_level


def map_levels(word, hsk_map):
    try:
        return hsk_map.value[word]
    except KeyError:
        return -1


def main():
    spark = utils.setup_spark()
    articles = utils.get_articles(spark)
    hsk_map = utils.get_hsk_dict(spark)

    articles = articles.limit(1000)

    out = articles.rdd.map(lambda article: (article['news_id'],
                                            [x.word for x in pseg.cut(article['content']) if x.flag not in ['x', 'eng', 'm']],
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
                                          words[6])) \
                      .map(lambda levels: (levels[0],
                                           evaluate(levels[1]),
                                           levels[2],
                                           levels[3],
                                           levels[4],
                                           levels[5],
                                           levels[6]))

    # out.toDF().groupBy('_2').count().show()
    out = out.toDF().select(col('_1').alias('news_id'), col('_2').alias('level'), col('_3').alias('source'),
                            col('_4').alias('source'), col('_5').alias('title'), col('_6').alias('keywords'),
                            col('_7').alias('desc'))

    out.toPandas().to_csv('../../output/full_sample/distributed/test.csv', index=False, sep='\t')


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(f'Duration: {(end-start)/60} min')

