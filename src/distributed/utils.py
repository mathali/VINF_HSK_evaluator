import json
import ast
import sys
import pandas as pd

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, ShortType
from pyspark.sql import functions as f, SparkSession
from os import walk
import os
import re


def get_articles(spark, mode='valid'):
    """
    Load the dataset into spark and return the reference to it

    :param spark: session created in setup_spark
    :param mode: determines which dataset (file) is loaded
    :return: Prepared dataset
    """

    # Predefined schema to speed up loading
    article_schema = StructType([
        StructField('content', StringType(), True),
        StructField('desc', StringType(), True),
        StructField('keywords', StringType(), True),
        StructField('news_id', StringType(), True),
        StructField('source', StringType(), True),
        StructField('time', StringType(), True),
        StructField('title', StringType(), True),
    ])

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Dataset loading
    if mode == 'valid' or mode == 'demo':
        # smallest versions, just prepare a basic dataset
        path = os.path.join(os.getcwd(), '..', '..', 'data', 'new2016zh', 'news2016zh_valid.json')
        return spark.read.json(path, schema=article_schema)
    elif mode == 'train' or mode == 'full':
        # version working with the full dataset
        # convert the dataset into parquet because it might be a bit faster to process (I think)
        path = os.path.join(os.getcwd(), '..', '..', 'data', 'new2016zh', 'news2016zh_train.parquet')
        if not os.path.isdir(path):
            json_path = os.path.join(os.getcwd(), '..', '..', 'data', 'new2016zh', 'news2016zh_train.json')
            create_parquet(spark, json_path)

        parquet_file = spark.read.format('parquet')\
                                 .schema(article_schema)\
                                 .load(path)
        parquet_file.createOrReplaceTempView('articlesParquet')
        if mode == 'train':
            return spark.sql('SELECT * FROM articlesParquet LIMIT 150000')
        else:
            return spark.sql('SELECT * FROM articlesParquet')
    elif mode == 'eval':
        # Create a completely separate structure for evaluation, due to it being a different dataset
        eval_schema = StructType([
            StructField('id', ShortType(), True),
            StructField('HSK_level', ShortType(), True),
            StructField('URL', StringType(), True),
            StructField('Title_EN', StringType(), True),
            StructField('Title_ZH', StringType(), True),
            StructField('Description', StringType(), True),
            StructField('content', StringType(), True),
        ])
        path = os.path.join(os.getcwd(), '..', '..', 'data', 'hskreading_tab.csv')
        return spark.read.format('csv') \
                         .option('sep', '\t')\
                         .schema(eval_schema)\
                         .load(path)


def get_hsk_dict(spark):
    """
    Retrieve HSK graded dictionary in a format usable by spark during the main evaluation.

    :param spark: session created in setup_spark
    :return:
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(os.getcwd(), '..', '..', 'data', 'hsk.json')

    # Bit of a workaround to be able to load list of separate json objects
    hsk_dict = spark.sparkContext.wholeTextFiles(path).map(lambda x: ast.literal_eval(x[1])) \
                                                      .map(lambda x: json.dumps(x))

    dict_schema = StructType([
        StructField('hanzi', StringType(), True),
        StructField('id', LongType(), True),
        StructField('level', ShortType(), True),
        StructField('pinyin', StringType(), True),
        StructField('translations', ArrayType(elementType=StringType()), True),
    ])

    hsk_dict = spark.read.json(hsk_dict, schema=dict_schema)

    # Need to create a mapping in the spark table to replace traditional dictionary functionality
    hsk_dict = hsk_dict.withColumn("json", f.create_map(["hanzi", "level"]))

    # Extract only the relevant mapping to reduce overhead
    extracted_dict = {}
    for x in [zipped for zipped in zip(*hsk_dict.select('json').collect())]:
        for entry in x:
            items = list(entry.items())
            extracted_dict[items[0][0]] = items[0][1]

    # Broadcast the new mapping so it can be used properly by RDD functions
    return spark.sparkContext.broadcast(extracted_dict)


def setup_spark():
    """
    Create a spark session capable of distributed processing.
    10 threads (5 cores)
    12g of total memory

    Capable of processing the full dataset
    :return:
    """

    spark = SparkSession.builder \
        .appName("ArticleEvaluator") \
        .config("spark.master", "local[10]") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.ui.showConsoleProgress", "true") \
        .getOrCreate()

    return spark


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


###############################################################
# Bonus grammar processing
###############################################################


def get_grammar(g_directory='../../data/grammar'):
    """
    Just a support function for loading all csv grammar files from https://github.com/infinyte7/Chinese-Grammar
    :param g_directory:
    :return:
    """
    file_names = next(walk(g_directory), (None, None, []))[2]

    grammar_df = pd.read_csv('../../data/grammar/hsk1.csv')
    for file in file_names[1:]:
        temp_df = pd.read_csv('../../data/grammar/'+file)
        pd.concat([grammar_df, temp_df])

    return grammar_df


def filter_grammar(g_directory='../../data/grammar'):
    """
    Pre filter grammar DF so it's easier to write rules for RegEx conversion
    :param g_directory:
    :return:
    """
    file_names = next(walk(g_directory), (None, None, []))[2]
    file_names.sort()

    grammar = []
    for file in file_names:
        grammar.append(pd.read_csv(g_directory+'/'+file, header=None))

    structures = pd.DataFrame(columns=['HSK_level', 'structure'])
    for ind, df in enumerate(grammar):
        # CSV files contain a bunch of duplicates, because they provide multiple examples for each rule
        temp_df = pd.DataFrame({'HSK_level': ind + 1,
                                'structure': df[5].drop_duplicates(keep='first').map(lambda x: x.lstrip('::').rstrip('::'))
                                })
        structures = pd.concat([structures, temp_df])

    structures.to_csv('../../data/filtered_grammar/filtered_grammar.csv', sep='\t')


def get_grammar_mapping(spark):
    """
    Load and broadcast character and PoS RegEx rules so they are usable by RDD functions
    :param spark:
    :return:
    """
    grammar_schema = StructType([
        StructField('level', ShortType(), True),
        StructField('char_map',  StringType(), True),
        StructField('pos_map',  StringType(), True),
    ])
    g_df = spark.read.option("delimiter", "\t").csv('../../data/filtered_grammar/grammar_mapping.csv', schema=grammar_schema)

    grammar_out = []
    for x in g_df.collect():
        grammar_out.append((x[0], x[1], x[2]))

    return spark.sparkContext.broadcast(grammar_out[1:])


def map_to_regex():
    pos_mapping = pd.read_csv('../../data/filtered_grammar/tmp_pos_mapping', delimiter='\t')
    filtered_grammar = pd.read_csv('../../data/filtered_grammar/filtered_grammar.csv', delimiter='\t')

    levels = []
    char_regex = []
    pos_regex = []

    # Chinese character filter
    pattern = re.compile('[\u4e00-\u9fff]+')

    for index, row in filtered_grammar.iterrows():
        missing_pos_flag = False
        tmp_c, tmp_p = [], []
        # Remove redundant characters
        struct = row['structure'].replace(' ', '').replace('?', '').replace('？', '').replace('::', '')
        # Splitting based on these characters lets us create separate character and PoS rules
        struct = re.split('[+，＋]|⋯⋯|……', struct)

        # When adding rules to Characters, add wildcard to PoS and vice-versa
        for part in struct:
            # Replace grammar rule terms like 'Noun' or 'Duration' with appropriate Jieba PoS tags
            if part in pos_mapping['POS'].values:
                tmp_p.extend(pos_mapping[pos_mapping['POS'] == part]['Tag'].values)
                if '|' in tmp_p[-1]:
                    tmp_p[-1] = '('+tmp_p[-1]+')'
                tmp_c.append('.{1,7}')
            elif pattern.match(part):
                tmp_p.append('.{1,3}')

                # Process more complex parts of the grammar rules, like alternatives and optional parts
                part = part.replace('/', '|').replace('／', '|')
                part = re.sub('[（(]', '[', part)
                part = re.sub('[)）]', ']？', part)
                part = re.sub('[a-zA-Z"]', '', part)

                if '|' in part:
                    tmp_c.append('('+part+')')
                else:
                    tmp_c.append(part)
            elif part == '':
                tmp_p.append('.{1,7}')
                tmp_c.append('.{1,7}')
            else:
                missing_pos_flag = True
                break

        if missing_pos_flag:
            continue

        levels.append(row['HSK_level'])
        char_regex.append(tmp_c.copy())
        pos_regex.append(tmp_p.copy())

    grammar_mapping_df = pd.DataFrame(list(zip(levels, char_regex, pos_regex)), columns=['level', 'char_map', 'pos_map'])
    grammar_mapping_df.to_csv('../../data/filtered_grammar/grammar_mapping.csv', sep='\t', index=False)


if __name__ == '__main__':
    map_to_regex()
