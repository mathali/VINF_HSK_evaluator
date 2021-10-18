import ArticleEvaluator as ae
import search_engine as se
import crawler.hskreading as c
import evaluation as e
from utils import get_articles, get_hsk_dict
import time
import logging
import click


def process_articles(sample):
    logging.basicConfig(filename='../logs/evaluator.log', level='INFO')

    articles = get_articles()
    if sample:
        output = '../output/demonstration/evaluated_articles_fixed.csv'
        articles = articles[:1000]
    else:
        output = '../output/demonstration/evaluated_articles_fixed.csv'

    hsk_dict = get_hsk_dict()

    start = time.time()
    article_difficulties, discarded_articles = ae.evaluate_articles(articles, hsk_dict, output_file=output)
    end = time.time()

    print("Number of articles per HSK level: ")
    logging.info("Number of articles per HSK level: ")
    print(article_difficulties)
    logging.info(article_difficulties)

    print("Number of discarded articles: ")
    logging.info("Number of discarded articles: ")
    print(discarded_articles)
    logging.info(discarded_articles)

    print(f"Total time: {(end - start) / 60} min")
    logging.info(f"Total time: {(end - start) / 60} min")


@click.command()
@click.option('-f', '--function', 'function', required=True, help='Program functionality. If you decide to specify '
                                                                  'parameters for a function, you have to specify all '
                                                                  'of them.')
# @click.option('-p', '--params', 'params', help='Required parameters for specified function. Comma separated, e.g.: 10,False,out.csv')
@click.option('-l', '--levels', 'levels', help='Get_levels parameter for function \'evaluate\'. ')
@click.option('-s', '--sample', 'sample', default='True', help='Sample parameter True/False')
# @click.option('-p', '--phrase', 'phrase', help='Chinese phrase for full-text search')
@click.option('-m', '--mode', 'mode', help='Mode of full-text search \'and\'/\'or\'')
@click.option('-z', '--size', 'size', help='Size of data for search engine \'sample\'/\'full_sample\'')
@click.option('-g', '--flag', 'flag', help='Multipurpose True/False flag')
def main(function, levels, sample, mode, size, flag):
    phrase = '我饿了'      # Please use this to send phrases to full-text search. Terminal only supports ASCII characters.
    if flag == 'True':
        flag = True
    else:
        flag = False

    if function == 'p':
        if sample == 'True':
            process_articles(True)
        else:
            process_articles(False)
    elif function == 'i':
        if size:
            se.create_index(size)
        else:
            se.create_index()
    elif function == 'idf':
        if size:
            se.create_index_tf_idf(size)
        else:
            se.create_index_tf_idf()
    elif function == 's':
        start = time.time()
        if mode or size or flag:
            se.text_search(phrase=phrase, mode=mode, size=size, idf_flag=flag)
        else:
            se.text_search(phrase=phrase)
        end = time.time()
        print(f'Search duration: {(end - start) / 60} min')
    elif function == 'c':
        c.crawl()
    elif function == 'e':
        if levels == 'True':
            e.evaluate(True)
        else:
            e.evaluate()
    else:
        print('Error: Invalid function')

if __name__ == '__main__':
    main()