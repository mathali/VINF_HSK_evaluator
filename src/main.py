import src.ArticleEvaluator as ae
from src.utils import get_articles, get_hsk_dict
import time
import logging

if __name__ == '__main__':
    logging.basicConfig(filename='../logs/evaluator.log', level='INFO')

    articles = get_articles()
    hsk_dict = get_hsk_dict()

    start = time.time()
    article_difficulties, discarded_articles = ae.evaluate_articles(articles, hsk_dict)
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