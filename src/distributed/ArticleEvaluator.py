import time
import multiprocessing as mp
import jieba.posseg as pseg
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
import json
import ast
import os


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


def newCols(word):
    try:
        return hsk_map.value[word]
    except KeyError:
        return -1

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

hsk_dict = hsk_dict.withColumn("json", f.create_map(["hanzi", "level"]))

extracted_dict = {}
for x in [zipped for zipped in zip(*hsk_dict.select('json').collect())]:
    for entry in x:
        items = list(entry.items())
        extracted_dict[items[0][0]] = items[0][1]

hsk_map = sc.broadcast(extracted_dict)


callnewColsUdf = f.udf(newCols, t.StringType())


test = articles.limit(1000)

start = time.time()

out = test.rdd.map(lambda article: (article['news_id'], [x.word for x in pseg.cut(article['content']) if x.flag not in ['x', 'eng', 'm']])) \
                  .map(lambda words: (words[0], [newCols(word) for word in words[1]])) \
                  .map(lambda levels: (levels[0],evaluate(levels[1])))

out.toDF().groupBy('_2').count().show()
end = time.time()
print(f'Duration: {(end-start)/60} min')

