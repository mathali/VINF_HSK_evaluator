import json
import pandas as pd
from os import walk
import os


def get_articles(file="..\\data\\news2016zh_valid.json", csv=False, delimiter=',', walk=False):
    if not csv:
        data_list = []
        with open(file, "r", encoding="utf-8") as valid_file:
            for entry in valid_file:
                data = json.loads(entry)
                data_list.append(data)

        return data_list
    elif csv and walk:
        pass  # TODO: Implement way to load the files separately - probably only needed in the second part
    else:
        data = pd.read_csv(file, delimiter=delimiter)
        return data


def get_hsk_dict(hsk_dict="..\\data\\hsk.json"):
    with open(hsk_dict, "r", encoding="utf-8") as hsk_file:
        hsk_list = json.load(hsk_file)

    level_dict = {}
    for hanzi in hsk_list:
        level_dict[hanzi["hanzi"]] = hanzi["level"]

    return level_dict


def get_grammar(g_directory='../data/grammar'):
    file_names = next(walk(g_directory), (None, None, []))[2]

    grammar_df = pd.read_csv('../data/grammar/hsk1.csv')
    for file in file_names[1:]:
        temp_df = pd.read_csv('../data/grammar/'+file)
        pd.concat([grammar_df, temp_df])

    return grammar_df


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
    split_file()
