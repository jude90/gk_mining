
from parse import get_ukeys
import requests
import json
from model import Base,DB_Session,Article,Comment,User
from collections import defaultdict
from multiprocessing.dummy import Pool
import multiprocessing
import time
import sys


def partition(pages):
    user_dct=defaultdict(list)
    for article , ukey_lst in pages:
        session = DB_Session()
        session.execute(
        Comment.__table__.insert(),
        [{'article':article, 'user':ukey} for ukey in ukey_lst]
        )
        session.commit()
        session.close()
        for ukey in ukey_lst:
            try:
                user_dct[ukey].append(article)
            except :
                user_dct[ukey]=[article]
    
    finally:pass 
    return user_dct

def foo_map(article):
    
    
    try:
        time.sleep(0.5)
        ukey_lst = list(set(get_ukeys(article)))
        
        return article,ukey_lst
    except Exception, e:
        print e,"failure in article:"+article
        time.sleep(100)
        return None

def foo_reduce(ukey):
    url='http://apis.guokr.com/community/user/%s.json'%ukey
    try:
        time.sleep(1)
        user_json=requests.get(url,headers=header).content
        profile = (json.loads(user_json))["result"]
    
        user = dict(
        ukey = profile["ukey"],
        blogs = profile["blogs_count"],
        posts = profile["posts_count"],
        answers = profile["answers_count"],
        questions= profile["questions_count"],
        followers = profile["followers_count"],
        followings = profile["followings_count"],
        activities = profile["activities_count"],

        answer_supports = profile["answer_supports_count"],
        date_created = profile["date_created"],
        )
        return user
    except Exception, e:
        print e,"failure user :"+ukey
        time.sleep(100)
        return None