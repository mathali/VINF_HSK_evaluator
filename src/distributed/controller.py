import click
import ArticleEvaluator as ae
import ArticleEvaluatorGrammar as aeg
import grammar_evaluation as ge
import evaluation as e
import lucene_index as li
import lucene_reader as lr
import hskreading as c


@click.command()
@click.option('-i', '--index', 'index', is_flag=True, help='Index articles using PyLucene')
@click.option('-r', '--reader', 'reader', is_flag=True, help='Query indexed articles')
@click.option('-e', '--evaluate', 'evaluate', is_flag=True, help='Evaluate the HSK level of articles')
@click.option('-v', '--visualize', 'visualize', is_flag=True, help='Visualize the performance of the evaluator')
@click.option('-c', '--crawl', 'crawl', is_flag=True, help='Extract evaluation dataset from hskreading.com')
def main(index, reader, evaluate, visualize, crawl):
    if index:
        grammar = input('Include grammar?(y/n): ')
        if grammar == 'n':
            li.run(False)
        else:
            li.run(True)
    elif reader:
        grammar = input('Include grammar?(y/n): ')
        if grammar == 'n':
            lr.run(False)
        else:
            lr.run(True)
    elif evaluate:
        grammar = input('Include grammar?(y/n): ')
        if grammar == 'n':
            ae.run()
        else:
            aeg.run()
    elif visualize:
        grammar = input('Include grammar?(y/n): ')
        if grammar == 'n':
            e.run()
        else:
            ge.run()
    elif crawl:
        c.crawl()


if __name__ == '__main__':
    main()
