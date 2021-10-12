import ArticleEvaluator as ae
from utils import get_articles, get_hsk_dict
import time
import pandas as pd
import click
import matplotlib.pyplot as plt


# @click.command
# @click.argument('get_levels', default=False)
def main(get_levels):
    if get_levels:
        articles = get_articles('../data/hskreading_tab.csv', csv=True, delimiter='\t')
        hsk_dict = get_hsk_dict()

        start = time.time()
        article_difficulties, discarded_articles = ae.evaluate_articles(articles, hsk_dict, '../output/evaluated_hskreading.csv', True)
        end = time.time()

    df = pd.read_csv('../output/evaluated_hskreading.csv')
    exact = df['Labeled Level'] == df['Evaluated Level']
    within_one = abs(df['Labeled Level'] - df['Evaluated Level']) <= 1
    larger = df['Evaluated Level'] > df['Labeled Level']
    smaller = df['Evaluated Level'] < df['Labeled Level']

    exact.value_counts().plot.bar()
    plt.show()
    within_one.value_counts().plot.bar()
    plt.show()
    larger.value_counts().plot.bar()
    plt.show()
    smaller.value_counts().plot.bar()
    plt.show()

    print(f"Accuracy: {exact.sum() / len(exact)}")
    print(f"Accuracy within one level: {within_one.sum() / len(within_one)}")
    print(f"Evaluated as higher level: {larger.sum() / len(larger)}")
    print(f"Evaluated as lower level: {smaller.sum() / len(smaller)}")


if __name__ == '__main__':
    main(False)
