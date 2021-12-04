import ArticleEvaluator as ae
import utils
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn
import os


def evaluate(get_levels='False'):
    # If we need to process the evaluation dataset again
    if get_levels == 'True':
        spark = utils.setup_spark()
        ae.evaluation()

    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv('../../output/full_sample/distributed/evaluated_eval_partitions.csv', delimiter='\t')
    df = df[df['Evaluated Level'] != -1]

    exact = df['Labeled Level'] == df['Evaluated Level']
    within_one = abs(df['Labeled Level'] - df['Evaluated Level']) <= 1
    larger = df['Evaluated Level'] > df['Labeled Level']
    smaller = df['Evaluated Level'] < df['Labeled Level']
    confusion_df = pd.crosstab(df['Labeled Level'], df['Evaluated Level'])
    confusion_df['6'] = 0
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

    plt.show()


def run():
    mode = input('Process articles? (True/False): ')
    evaluate(mode)


if __name__ == '__main__':
    run()
