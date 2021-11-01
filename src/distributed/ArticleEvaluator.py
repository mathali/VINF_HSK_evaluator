import time
from itertools import chain
import multiprocessing as mp

import jieba.posseg as pseg
import jieba
from pyspark.sql.functions import create_map, lit

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from functools import partial
import json
import ast
import os
import pandas as pd


def process_article(x, hsk_dict):
    seg_list = pseg.cut(x['content'])
    seg_list = [x for x in seg_list if x.flag not in ['x', 'eng', 'm']]

    level_count = {
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
    }

    for s in seg_list:
        try:
            level = hsk_dict.filter(hsk_dict.hanzi == s.word).collect()[0][2]
            level_count[level] += 1
        except IndexError:
            continue

    current_level = 1
    current_count = level_count[current_level]
    total_level = sum(level_count.values())

    if total_level == 0:
        return -1

    # If article contains at least 80% of vocabulary from a current HSK level or lower, graded it as current HSK level
    # Based on 80-20 known-unknown rule for optimal language acquisition
    ratio = current_count / total_level
    while ratio < 0.8:
        current_level += 1
        current_count += level_count[current_level]
        ratio = current_count / total_level

    return current_level


def get_level(word):
    return hsk_dict.filter(hsk_dict.hanzi == word).select('level')
    # levels = []
    #hsk_dict.filter(hsk_dict.hanzi == word).select('level')
    # for word in words:
    #     try:
    #         levels.append(hsk_dict.filter(hsk_dict.hanzi == word).collect()[0][2])
    #     except IndexError:
    #         levels.append(-1)
    #
    # return levels


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

os.environ["PYSPARK_PYTHON"]="/home/matej/PycharmProjects/pythonProject/venv/bin/python3"


spark = SparkSession.builder.master("local[1]") \
    .appName("ArticleEvaluator").getOrCreate()

sc = spark.sparkContext

hsk_dict = sc.wholeTextFiles('../../data/hsk.json').map(lambda x:ast.literal_eval(x[1]))\
                                                   .map(lambda x: json.dumps(x))

hsk_dict = spark.read.json(hsk_dict)
hsk_dict.printSchema()

articles = spark.read.json('../../data/new2016zh/news2016zh_valid.json')
articles.printSchema()
# for row in articles.rdd.collect():
#     print(process_article(row, hsk_dict ))

# out = articles.rdd.map(lambda x: process_article(x, hsk_dict))
# out = out.take(5)
# print(out)
# out.show()

hsk_dict = hsk_dict.withColumn("json", f.create_map(["hanzi", "level"]))
# for x in zip(*hsk_dict.select('json').collect()):
#     print(x[0])

extracted_dict = {}
for x in [zipped for zipped in zip(*hsk_dict.select('json').collect())]:
    for entry in x:
        items = list(entry.items())
        extracted_dict[items[0][0]] = items[0][1]

hsk_map = sc.broadcast(extracted_dict)

def newCols(word):
    try:
        return hsk_map.value[word]
    except KeyError:
        return -1

callnewColsUdf = f.udf(newCols, t.StringType())


test = articles.limit(1000)

start = time.time()
pool = mp.Pool(5)

out = test.rdd.map(lambda article: (article['news_id'], [x.word for x in pseg.cut(article['content']) if x.flag not in ['x', 'eng', 'm']])) \
                  .map(lambda words: (words[0], [newCols(word) for word in words[1]])) \
                  .map(lambda levels: (levels[0],evaluate(levels[1])))
                  #.map(lambda words: [hsk_dict.filter(hsk_dict.hanzi == word).select('level').collect() for word in words])
                  #.map(lambda levels: evaluate(levels))

out.toDF().groupBy('_2').count().show()
end = time.time()
print(f'Duration: {(end-start)/60} min')
#out.groupBy(0).count().show()
# for x in hsk_dict.select('json').collect():
#     print(x[0]['爱'])
#     break
# for row in out.take(1):
#     for word in row:
#
    #print([hsk_dict.filter(hsk_dict.hanzi == word).select('level').collect() for word in row])
    # for word in row:
    #     print(hsk_dict.filter(hsk_dict.hanzi == word).select('level').collect())

# for row in test.rdd.take(5):
#     seg_list = [x.word for x in pseg.cut(row['content']) if x.flag not in ['x', 'eng', 'm']]
#     words = get_level(seg_list)
#     levels = evaluate(words)
#     print(levels)

