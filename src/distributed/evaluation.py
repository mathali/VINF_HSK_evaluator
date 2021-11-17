import ArticleEvaluator as ae
import utils
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn


def evaluate(get_levels=False):
    if get_levels:
        spark = utils.setup_spark()
        ae.evalulation('eval')

    df = pd.read_csv('../../output/full_sample/distributed/evaluated_eval_partitions.csv', delimiter='\t')
    df = df[df['Evaluated Level'] != -1]

    exact = df['Labeled Level'] == df['Evaluated Level']
    within_one = abs(df['Labeled Level'] - df['Evaluated Level']) <= 1
    larger = df['Evaluated Level'] > df['Labeled Level']
    smaller = df['Evaluated Level'] < df['Labeled Level']
    confusion_df = pd.crosstab(df['Labeled Level'], df['Evaluated Level'])

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

    plt.figure(figsize=(5, 3.5))
    sn.heatmap(confusion_df, annot=True)
    plt.title('Confusion matrix')
    plt.rcParams.update({'font.size': 22})

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
