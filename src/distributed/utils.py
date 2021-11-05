import json
import ast
import sys

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, ShortType
from pyspark.sql import functions as f, SparkSession
from os import walk
import os


def get_articles(spark, eval_set=False):
    if not eval_set:
        article_schema = StructType([
            StructField('content', StringType(), True),
            StructField('desc', StringType(), True),
            StructField('keywords', StringType(), True),
            StructField('news_id', StringType(), True),
            StructField('source', StringType(), True),
            StructField('time', StringType(), True),
            StructField('title', StringType(), True),
        ])
        return spark.read.json('../../data/new2016zh/news2016zh_valid.json', schema=article_schema)


def get_hsk_dict(spark):
    hsk_dict = spark.sparkContext.wholeTextFiles('../../data/hsk.json').map(lambda x: ast.literal_eval(x[1])) \
                                                                       .map(lambda x: json.dumps(x))

    dict_schema = StructType([
        StructField('hanzi', StringType(), True),
        StructField('id', LongType(), True),
        StructField('level', ShortType(), True),
        StructField('pinyin', StringType(), True),
        StructField('translations', ArrayType(elementType=StringType()), True),
    ])

    hsk_dict = spark.read.json(hsk_dict, schema=dict_schema)

    hsk_dict = hsk_dict.withColumn("json", f.create_map(["hanzi", "level"]))

    extracted_dict = {}
    for x in [zipped for zipped in zip(*hsk_dict.select('json').collect())]:
        for entry in x:
            items = list(entry.items())
            extracted_dict[items[0][0]] = items[0][1]

    return spark.sparkContext.broadcast(extracted_dict)


def setup_spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .master("local[8]") \
        .appName("ArticleEvaluator") \
        .getOrCreate()

    return spark
    # conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '8g'),
    #                                         ('spark.app.name', 'ArticleEvaluator'),
    #                                         ('spark.executor.cores', '12'),
    #                                         ('spark.cores.max', '12'),
    #                                         ('spark.driver.memory', '6g')])
    #
    # spark.sparkContext.stop()
    # return SparkSession.builder.config(conf=conf).getOrCreate()


def get_grammar(g_directory='../data/grammar'):
    file_names = next(walk(g_directory), (None, None, []))[2]

    grammar_df = pd.read_csv('../data/grammar/hsk1.csv')
    for file in file_names[1:]:
        temp_df = pd.read_csv('../data/grammar/'+file)
        pd.concat([grammar_df, temp_df])

    return grammar_df


def filter_grammar(g_directory='../data/grammar'):
    file_names = next(walk(g_directory), (None, None, []))[2]

    grammar = []
    for file in file_names[1:]:
        grammar.append(pd.read_csv(g_directory+'/'+file, header=None))

    structures = pd.DataFrame(columns=['HSK_level', 'structure'])
    for ind, df in enumerate(grammar):
        temp_df = pd.DataFrame({'HSK_level': ind + 1,
                                'structure': df[5].drop_duplicates().map(lambda x: x.lstrip('::').rstrip('::')).str.split("+")
                                })
        structures = pd.concat([structures, temp_df])

    structures.to_csv('../data/grammar/filtered_grammar.csv')


def split_file(file='D:/Dokumenty/FIIT/ing/1.semester/VINF/new2016zh/news2016zh_train.json',
               lines=True,
               chunk_size=50000):

    chunks = pd.read_json(file, lines=lines, chunksize=chunk_size)
    out_dir = '../data/split/'+file.split('/')[-1].split('.')[0]

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    for ind, chunk in enumerate(chunks):
        chunk.to_csv(out_dir+f'/chunk_{str(ind).zfill(3)}.csv')


if __name__ == '__main__':
    filter_grammar()
