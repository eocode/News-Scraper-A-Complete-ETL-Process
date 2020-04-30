import hashlib
import logging
from urllib.parse import urlparse

import nltk
import pandas as pd
from nltk.corpus import stopwords

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Source:
    def __init__(self, dataset, newspaper_uid):
        self.dataset = dataset
        self.dataraw = None
        self.newspaper_uid = newspaper_uid
        self.__load()

    def __load(self):
        logger.info(f'Reading dataset: {self.dataset}!!')
        self.dataraw = pd.read_csv(self.dataset,
                                   encoding='utf-8', sep=',')


def add_newspaper_uid(df, newspaper_uid):
    logger.info(f'Filling newspaper_uid column with {newspaper_uid}!!')
    df['newspaper_uid'] = 'eluniversal'
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df


def extract_host(df):
    logger.info(f'Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df


def fill_missing_titles(df):
    logger.info(f'Filling missing titles')
    missing_titles_mask = df['title'].isna()
    missing_titles = (df[missing_titles_mask]['url']
                      .str.extract(r'(?P<missing_titles>[^/]+)$')
                      .applymap(lambda title: title.split('-'))
                      .applymap(lambda title_word_list: ' '.join(title_word_list))
                      )
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df


def generate_ids_for_rows(df):
    logger.info(f'Generating rid for each row')
    rid = (df
           .apply(lambda row: hashlib.sha1(bytes(row['url'].encode())), axis=1)
           .apply(lambda hash_object: hash_object.hexdigest())
           )
    df['uid'] = rid
    return df.set_index('uid')


def remove_new_lines_from_body(df):
    logger.info(f'Remove new lines from body')
    stripped_body = (df
                     .apply(lambda row: row['content'], axis=1)
                     .apply(lambda content: list(content))
                     .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ''), letters)))
                     .apply(lambda letters: list(map(lambda letter: letter.replace('\r', ''), letters)))
                     .apply(lambda letters: ''.join(letters))
                     )
    df['content'] = stripped_body
    return df


def tokenize_column(df, column_name):
    logger.info('Calculating the number of unique tokens in {}'.format(column_name))
    stop_words = set(stopwords.words('spanish'))

    n_tokens = (df
                .dropna()
                .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
                .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
                .apply(lambda valid_word_list: len(valid_word_list))
                )

    df['n_tokens_' + column_name] = n_tokens

    return df


def remove_duplicate_entries(df, column_name):
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df


def drop_rows_with_missing_values(df):
    logger.info('Dropping rows with missing values')
    return df.dropna()


def save_data(df, filename):
    file = filename.replace('.csv', '')
    clean_filename = f'{file}_clean.csv'
    logger.info(f'Saving data at location {filename}')
    df.to_csv(clean_filename)


class DataWrangling(Source):
    def __init__(self, dataset, newspaper_uid):
        super().__init__(dataset, newspaper_uid)
        self.process()

    def process(self):
        logger.info('Start transformations!!')
        df = self.dataraw
        df = add_newspaper_uid(df, self.newspaper_uid)
        df = extract_host(df)
        df = fill_missing_titles(df)
        df = generate_ids_for_rows(df)
        df = remove_new_lines_from_body(df)
        df = tokenize_column(df, 'title')
        df = tokenize_column(df, 'content')
        df = remove_duplicate_entries(df, 'title')
        df = drop_rows_with_missing_values(df)
        save_data(df, self.dataset)
        print(df)
