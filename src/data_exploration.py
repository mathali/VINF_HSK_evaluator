import json
import os
import heapq
import jieba
import jieba.posseg as pseg
import time
import csv
import logging
from stopwordsiso import stopwords


logging.basicConfig(filename='../logs/non-parallel.log')

data_list = []
with open("..\\data\\news2016zh_valid.json", "r", encoding="utf-8") as valid_file:
    for entry in valid_file:
        data = json.loads(entry)
        data_list.append(data)


with open("..\\data\\hsk.json", "r", encoding="utf-8") as hsk_file:
    hsk_list = json.load(hsk_file)

level_dict = {}
for hanzi in hsk_list:
    level_dict[hanzi["hanzi"]] = hanzi["level"]

# seg_list = jieba.cut(data_list[0]['content'], cut_all=True)
# print("Full Mode: " + "/ ".join(seg_list))  # 全模式
# seg_list = pseg.cut(data_list[0]['content'])
# print("Default Mode: " + "/ ".join(seg_list))


article_difficulties = {
    'HSK1': 0,
    'HSK2': 0,
    'HSK3': 0,
    'HSK4': 0,
    'HSK5': 0,
    'HSK6': 0,
}

start = time.time()
print(os.getcwd())
with open('../output/non-parallel.csv', 'w', newline='', encoding='utf-8') as output_file:
    writer = csv.writer(output_file)
    for article in data_list:
        seg_list = pseg.cut(article['content'])
        id = article['news_id']

        seg_list = [x for x in seg_list if x.flag not in ['x', 'eng']]

        level_count = {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
            #7: 0,
        }

        total_level = 0

        for s in seg_list:
            try:
                level_count[level_dict[s.word]] += 1
            except:
                # level_count[7] += 1
                continue

        current_level = 1
        current_count = level_count[current_level]
        total_level = sum(level_count.values())

        if total_level == 0:
            # print("Invalid article")
            continue

        ratio = current_count / total_level
        while ratio < 0.8:
            current_level += 1
            current_count += level_count[current_level]
            ratio = current_count / total_level

        article_difficulties['HSK'+str(current_level)] += 1
        writer.writerow([id, current_level])#, article['content']])
        #print(f'Article level: HSK{current_level}+')
        #print(ratio)

end = time.time()

print("Number of articles per HSK level: ")
logging.info("Number of articles per HSK level: ")
print(article_difficulties)
logging.info(article_difficulties)
print(f"Total time: {(end-start)/60} min")
logging.info(f"Total time: {(end-start)/60} min")



