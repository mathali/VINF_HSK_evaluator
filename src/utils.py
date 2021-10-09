import json
import pandas as pd
from os import walk

def get_articles(file="..\\data\\news2016zh_valid.json", eval=False):
    if not eval:
        data_list = []
        with open(file, "r", encoding="utf-8") as valid_file:
            for entry in valid_file:
                data = json.loads(entry)
                data_list.append(data)

        return data_list
    else:
        data = pd.read_csv(file, delimiter='\t')
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


if __name__ == '__main__':
    get_grammar()
