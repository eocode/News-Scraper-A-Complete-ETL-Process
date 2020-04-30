import csv
import datetime
import logging
import re

import pandas as pd
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

from common import config
from models.etl.extract.news_page_object import HomePage, ArticlePage
from models.etl.extract.post import Post
from models.etl.load.data_model import Article
from models.etl.load.sqlite import Base, Engine, Session
from models.etl.transform.datawrangling import DataWrangling

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

is_well_formed_url = re.compile(r'^https?://.+/.+$')
is_root_path = re.compile(r'^/.+$')


class Pipeline:
    @staticmethod
    def extract_data(news_site_uid, host):
        logger.info('Start extracting!!')
        canting = int(input('How much news do you need? '))
        option = input('Do you want search news by specific words? (y/n) ')
        with_search = False
        if option.lower() == 'y':
            with_search = True
            searching = input('what do you want to search? (Enter to search all) ')
            host = host + config()['news_sites'][news_site_uid]['search'] + searching

        homepage = HomePage(news_site_uid, host, with_search)
        articles = []
        for link in homepage.article_links:
            article = Pipeline.fetch_article(news_site_uid, host, link)

            if article:
                logger.info('Article fetched!!')
                post = Post(article.title, article.body, Pipeline.build_link(host, link))
                articles.append(post)

            if len(articles) == canting:
                break
        Pipeline.save_articles(news_site_uid, articles)

    @staticmethod
    def transform_data(news_site_uid):
        logger.info('Transforming data!!')
        DataWrangling(Pipeline.get_output_file(news_site_uid, False), news_site_uid)

    @staticmethod
    def load_data(news_site_uid):
        logger.info('Loading data!!')
        Base.metadata.create_all(Engine)
        session = Session()
        articles = pd.read_csv(Pipeline.get_output_file(news_site_uid, True))

        for index, row in articles.iterrows():
            logger.info(f'Loading article uid {row["uid"]} into DB')
            article = Article(row['uid'],
                              row['content'],
                              row['host'],
                              row['newspaper_uid'],
                              row['n_tokens_content'],
                              row['n_tokens_title'],
                              row['title'],
                              row['url'])
            session.add(article)

        session.commit()
        session.close()

    @staticmethod
    def get_output_file(news_site_uid, clean):
        now = datetime.datetime.now().strftime('%Y_%m_%d')
        if clean:
            out_file_name = f'data/{news_site_uid}_{now}_articles_clean.csv'
        else:
            out_file_name = f'data/{news_site_uid}_{now}_articles.csv'
        return out_file_name

    @staticmethod
    def save_articles(news_site_uid, articles):
        csv_headers = Post.get_csv_headers()
        print(csv_headers)
        with open(Pipeline.get_output_file(news_site_uid, False), mode='w+', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(csv_headers)

            for article in articles:
                writer.writerow(article.get_list_for_csv())

    @staticmethod
    def fetch_article(news_site_uid, host, link):
        logger.info(f'Start fetching article at {link}')

        article = None
        try:
            article = ArticlePage(news_site_uid, Pipeline.build_link(host, link))
        except (HTTPError, MaxRetryError) as e:
            logger.warning('Errro while fetching the article', exc_info=False)

        if article and not article.body:
            logger.warning('Invalid article. There is no body')
            return None

        return article

    @staticmethod
    def build_link(host, link):
        if is_well_formed_url.match(link):
            return link
        elif is_root_path.match(link):
            return '{host}{link}'.format(host=host, link=link)
        else:
            return '{host}/{link}'.format(host=host, link=link)
