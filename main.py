import argparse
import logging

from common import config
from models.data_engineering_pipeline import Pipeline

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def _news_scraper(news_site_uid, mode):
    host = config()['news_sites'][news_site_uid]['url']
    logging.info(f'Beginning scraper for {host}')

    if mode == 'e':
        Pipeline.extract_data(news_site_uid, host)
    elif mode == 't':
        Pipeline.transform_data(news_site_uid)
    elif mode == 'l':
        Pipeline.load_data(news_site_uid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    news_site_choices = list(config()['news_sites'].keys())
    parser.add_argument('mode',
                        help='Indicate etl Process (e), (t), (l)',
                        type=str,
                        choices=['e', 't', 'l'])
    parser.add_argument('news_site',
                        help='The news site that you want to scraper',
                        type=str,
                        choices=news_site_choices)

    args = parser.parse_args()
    _news_scraper(args.news_site, args.mode)
