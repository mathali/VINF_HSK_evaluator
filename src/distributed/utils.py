import json
import ast
import sys
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, ShortType
from pyspark.sql import functions as f, SparkSession
from os import walk
import os
import re


def get_articles(spark, mode='valid'):
    article_schema = StructType([
        StructField('content', StringType(), True),
        StructField('desc', StringType(), True),
        StructField('keywords', StringType(), True),
        StructField('news_id', StringType(), True),
        StructField('source', StringType(), True),
        StructField('time', StringType(), True),
        StructField('title', StringType(), True),
    ])

    if mode == 'valid':
        return spark.read.json('../../data/new2016zh/news2016zh_train.json', schema=article_schema)
    elif mode == 'train':
        parquet_file = spark.read.format('parquet')\
                                 .schema(article_schema)\
                                 .load('../../data/new2016zh/news2016zh_train.parquet')
        parquet_file.createOrReplaceTempView('articlesParquet')
        return spark.sql('SELECT * FROM articlesParquet LIMIT 150000')
    elif mode == 'eval':
        eval_schema = StructType([
            StructField('id', ShortType(), True),
            StructField('HSK_level', ShortType(), True),
            StructField('URL', StringType(), True),
            StructField('Title_EN', StringType(), True),
            StructField('Title_ZH', StringType(), True),
            StructField('Description', StringType(), True),
            StructField('content', StringType(), True),
        ])
        return spark.read.format('csv') \
                         .option('sep', '\t')\
                         .schema(eval_schema)\
                         .load('../../data/hskreading_tab.csv')


def create_parquet(spark, original):
    dest = original.split('/')
    name = dest[-1].split('.')[0]
    format = dest[-1].split('.')[1]
    path = '/'.join(dest[:-1])

    if format == 'json':
        article_schema = StructType([
            StructField('content', StringType(), True),
            StructField('desc', StringType(), True),
            StructField('keywords', StringType(), True),
            StructField('news_id', StringType(), True),
            StructField('source', StringType(), True),
            StructField('time', StringType(), True),
            StructField('title', StringType(), True),
        ])
        org_file = spark.read.json(original, schema=article_schema)
        org_file.write.parquet(path + '/' + name + '.parquet')


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
        .master("local[10]") \
        .appName("ArticleEvaluator") \
        .getOrCreate()

    return spark


def get_grammar(g_directory='../../data/grammar'):
    file_names = next(walk(g_directory), (None, None, []))[2]

    grammar_df = pd.read_csv('../../data/grammar/hsk1.csv')
    for file in file_names[1:]:
        temp_df = pd.read_csv('../../data/grammar/'+file)
        pd.concat([grammar_df, temp_df])

    return grammar_df


def filter_grammar(g_directory='../../data/grammar'):
    file_names = next(walk(g_directory), (None, None, []))[2]
    file_names.sort()

    grammar = []
    for file in file_names:
        grammar.append(pd.read_csv(g_directory+'/'+file, header=None))

    structures = pd.DataFrame(columns=['HSK_level', 'structure'])
    for ind, df in enumerate(grammar):
        temp_df = pd.DataFrame({'HSK_level': ind + 1,
                                'structure': df[5].drop_duplicates(keep='first').map(lambda x: x.lstrip('::').rstrip('::'))
                                })
        structures = pd.concat([structures, temp_df])

    structures.to_csv('../../data/filtered_grammar/filtered_grammar.csv', sep='\t')


# TODO: Fix splitting issue in certain rules such as 194 and 192
# TODO: Detect alternatives marked with '/'
# TODO: Detect optional elements marked with '()'
def map_to_regex():
    pos_mapping = pd.read_csv('../../data/filtered_grammar/tmp_pos_mapping', delimiter='\t')
    filtered_grammar = pd.read_csv('../../data/filtered_grammar/filtered_grammar.csv', delimiter='\t')

    levels = []
    char_regex = []
    pos_regex = []

    pattern = re.compile('[\u4e00-\u9fff]+')

    for index, row in filtered_grammar.iterrows():
        missing_pos_flag = False
        tmp_c, tmp_p = [], []
        struct = row['structure'].replace(' ', '').split('+')
        for part in struct:
            if part in pos_mapping['POS'].values:
                tmp_p.extend(pos_mapping[pos_mapping['POS'] == part]['Tag'].values)
                tmp_c.append('.')
            elif pattern.match(part):
                tmp_p.append('.')
                tmp_c.append(part)
            else:
                missing_pos_flag = True
                break

        # print(tmp_p)
        # print(tmp_c)
        # print('========')

        if missing_pos_flag:
            continue

        levels.append(row['HSK_level'])
        char_regex.append(tmp_c.copy())
        pos_regex.append(tmp_p.copy())

    grammar_mapping_df = pd.DataFrame(list(zip(levels, char_regex, pos_regex)), columns=['level', 'char_map', 'pos_map'])
    grammar_mapping_df.to_csv('../../data/filtered_grammar/grammar_mapping.csv', sep='\t')

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
    map_to_regex()
