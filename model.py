from sqlalchemy.orm import sessionmaker ,mapper
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

from sqlalchemy.ext.declarative import declarative_base

DB_CONNECT_STRING ='mysql+mysqldb://root:@localhost/gk?charset=utf8'
engine = create_engine(DB_CONNECT_STRING,echo=False)
DB_Session = sessionmaker(bind= engine)
session = DB_Session()
Base = declarative_base()


class Article(Base):
    __tablename__ ='articles'

    id = Column(Integer,primary_key=True)

    location = Column(String(255))



class Comment(Base):
    __tablename__ ='comments'

    id = Column(Integer,primary_key=True)
    article = Column(String(255),ForeignKey("articles.location",ondelete='CASCADE', onupdate='CASCADE'))
    user = Column(String(255),ForeignKey("users.ukey",ondelete='CASCADE', onupdate='CASCADE'))

class User(Base):
    __tablename__ = 'users';

    id = Column(Integer, primary_key=True)
    ukey = Column(String(255));
    blogs = Column(Integer);
    posts = Column(Integer);
    answers = Column(Integer);
    questions = Column(Integer);
    followers = Column(Integer);
    followings = Column(Integer);
    
    activities = Column(Integer);

    answer_supports = Column(Integer);
    
    date_created = Column(String(255));


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    


