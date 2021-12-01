import click
import ArticleEvaluator as ae
import evaluation as e
import lucene_index as li
import lucene_reader as lr


@click.command()
@click.option('-i', '--index', 'index', is_flag=True, help='Index articles using PyLucene')
@click.option('-r', '--reader', 'reader', is_flag=True, help='Query indexed articles')
@click.option('-e', '--evaluate', 'evaluate', is_flag=True, help='Evaluate the HSK level of articles')
@click.option('-v', '--visualize', 'visualize', is_flag=True, help='Visualize the performance of the evaluator')
def main(index, reader, evaluate, visualize):
    if index:
        li.run()
    elif reader:
        lr.run()
    elif evaluate:
        ae.run()
    elif visualize:
        e.run()


if __name__ == '__main__':
    main()
