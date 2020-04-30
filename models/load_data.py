from sqlalchemy import Column, String

from models.sqlite import Base


class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)
    content = Column(String)
    host = Column(String)
    title = Column(String)
    newspaper_uid = Column(String)
    n_tokens_content = Column(String)
    n_tokens_title = Column(String)
    url = Column(String, unique=True)

    def __init__(self,
                 uid, content, host, newspaper_uid, n_tokens_content, n_tokens_title, title, url):
        self.id = uid
        self.content = content
        self.host = host
        self.newspaper_uid = newspaper_uid
        self.n_tokens_content = n_tokens_content
        self.n_tokens_title = n_tokens_title
        self.title = title
        self.url = url
