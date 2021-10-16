import ArticleEvaluator as ae
from utils import get_articles, get_hsk_dict
import pandas as pd
import matplotlib.pyplot as plt


def evaluate(get_levels=False):
    if get_levels:
        articles = get_articles('../data/hskreading_tab.csv', csv=True, delimiter='\t')
        hsk_dict = get_hsk_dict()

        ae.evaluate_articles(articles, hsk_dict, '../output/evaluated_hskreading.csv', True)

    df = pd.read_csv('../output/evaluated_hskreading.csv', delimiter='\t')
    exact = df['Labeled Level'] == df['Evaluated Level']
    within_one = abs(df['Labeled Level'] - df['Evaluated Level']) <= 1
    larger = df['Evaluated Level'] > df['Labeled Level']
    smaller = df['Evaluated Level'] < df['Labeled Level']

    fig = plt.figure(figsize=(8, 6))

    ax = fig.add_subplot(2, 2, 1)
    plt.pie(exact.value_counts(), labels=['False', 'True'], autopct='%.2f')
    plt.title('Accuracy')

    ax = fig.add_subplot(2, 2, 2)
    plt.pie(within_one.value_counts(), labels=['True', 'False'], autopct='%.2f')
    plt.title('Accuracy within one level')

    ax = fig.add_subplot(2, 2, 3)
    plt.pie(larger.value_counts(), labels=['False', 'True'], autopct='%.2f')
    plt.title('Evaluated as higher level')

    ax = fig.add_subplot(2, 2, 4)
    plt.pie(smaller.value_counts(), labels=['False', 'True'], autopct='%.2f')
    plt.title('Evaluated as lower level')

    # accuracy = exact.sum() / len(exact)
    # acc_within_one = within_one.sum() / len(within_one)
    # higher = larger.sum() / len(larger)
    # lower = smaller.sum() / len(smaller)
    #
    # ax = fig.add_subplot(3, 1, 2)
    # ax.set_axis_off()
    # ax.table(cellText=[[f'{accuracy*100:1.1f}%'],
    #                             [f'{acc_within_one*100:1.1f}%'],
    #                             [f'{higher*100:1.1f}%'],
    #                             [f'{lower*100:1.1f}%']],
    #                   rowLabels=['Accuracy',
    #                              'Accuracy within one level',
    #                              'Evaluated as higher level',
    #                              'Evaluated as lower level']).auto_set_column_width(0)

    plt.show()


if __name__ == '__main__':
    evaluate(False)
