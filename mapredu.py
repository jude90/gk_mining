
from parse import get_ukeys
import requests
import json
from model import Base,DB_Session,Article,Comment,User
from collections import defaultdict
from multiprocessing.dummy import Pool
import multiprocessing
import time
import sys

from datetime import datetime
from random import random
from  pipe import *

header={"User-Agent":
        '''Mozilla/5.0 (Windows NT 5.1) 
        AppleWebKit/537.36 (KHTML, like Gecko) 
        Chrome/30.0.1599.101 Safari/537.36'''
        }
pool =Pool(multiprocessing.cpu_count()*3)

#@profile
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
        del(session)
        
        for ukey in ukey_lst:
            try:
                user_dct[ukey].append(article)
            except :
                user_dct[ukey]=[article]
            
    return user_dct


#@profile
def foo_map(article):
    
    
    try:
        
        ukey_lst = list(set(get_ukeys(article)))
        time.sleep(random())
        print "get article",article
        return article,ukey_lst
    except Exception, e:
        print >>log, e,"failure in article:"+article
        time.sleep(100)
        return None
    
    

    

#@profile
def foo_reduce(ukey):
    url='http://apis.guokr.com/community/user/%s.json'%ukey
    try:
        
        user_json=requests.get(url,headers=header).content
        time.sleep(random())
        print "get user" ,ukey
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
        print  >>log , e,"failure user :"+ukey
        time.sleep(100)
        return None
    
    
def chunks(arr, n):
    return [arr[i:i+n] for i in range(0, len(arr), n)]
    

#@profile
def main():    
    urls = json.load(open("urls.txt",'r'))
    
    page_set = pool.map(foo_map, urls)|where(lambda x:x)
    
    pdb.set_trace()
    #ukeys = partition(filter(lambda x :x ,page_set))
    ukeys = partition(page_set)
    #pool.waite()
    ##~~~~~~~~~
    #pdb.set_trace()
    ##~~~~~~~~~
    with open("ukeys.txt",'w') as fuk:
        json.dump(ukeys,fuk,indent=4)
    
    print "finish articles !"

    users = pool.map(foo_reduce,ukeys.iterkeys())| where(lambda x :x)|as_tuple
    users_lst = chunks(users,1000)
    session = DB_Session()
    for us in users_lst:
        session.execute(
            User.__table__.insert(),
            us
            #pool.map(foo_reduce,ukeys.iterkeys())
        ) 
        session.commit()
    session.close()



if __name__ == '__main__':
    import pdb; 

#    DB_CONNECT_STRING ='mysql+mysqldb://root:@localhost/gk?charset=utf8'
#    engine = create_engine(DB_CONNECT_STRING,echo=True)
#    DB_Session = sessionmaker(bind= engine)
    print "start at " ,datetime.now()  
    log = open("crawlog.txt",'a')
    main()
    '''
    with open("ukeys.txt") as fu:
        ukeys =json.load(fu)

    users = pool.map(foo_reduce,ukeys.keys()[200:500])| where(lambda x :x)|as_tuple
    users_lst = chunks(users,13)
    session = DB_Session()
    for us in users_lst:
        session.execute(
            User.__table__.insert(),
            us
            #pool.map(foo_reduce,ukeys.iterkeys())
        )
        session.commit()
    session.close()
    '''
    '''uukeys=['za4yxz', 'whpaai', 'whpaai', '6g4qev', 'kshjrm', 'wpwjru', 'idqu9l', 'j52hfj', 'lq7fqy', 'r7wdwu', 'za4yxz', '104am6', 'dnjmmr', 'wjfeyk', 'jobman', 'hzcfnk', 'hzcfnk', 'bqx9aa', 'udk3j8', 'ifv1x3', '40c8cm', 'ai606k', 's9anto', 'xfbhhk', 'kfm15h', '77ti3u', 'jgwvuw', 'cyhrbd', 'h8zga6', 'qeko3g', '6tbqv2', 'za4yxz', '6tbqv2', 'e900sl', 'mflxkk', 'jxn1mp', '165696', '5vowdn', 'pan4yn', 'l2kho8', '4d8ooe', '4hjui8', 'sk8yhj', 'vkq5fk', 'n5h8e9', 'rmd0v0', 'z6qbkm', 'toxrf4', 'za4yxz', 'whpaai', 'whpaai', '6g4qev', 'kshjrm', 'wpwjru', 'idqu9l', 'j52hfj', 'lq7fqy', 'r7wdwu', 'za4yxz', '104am6', 'dnjmmr', 'wjfeyk', 'jobman', 'hzcfnk', 'hzcfnk', 'bqx9aa', 'udk3j8', 'ifv1x3', '40c8cm', 'ai606k', 's9anto', 'xfbhhk', 'kfm15h', '77ti3u', 'jgwvuw', 'cyhrbd', 'h8zga6', 'qeko3g', '6tbqv2'] 
    pool =Pool(multiprocessing.cpu_count()*2)
    users = pool.map(foo_reduce,uukeys)
    session = DB_Session()

    session.execute(
        User.__table__.insert(),
        users
        #pool.map(foo_reduce,ukeys.iterkeys())
    ) 
    session.commit()
    session.close() 
    '''
    '''    
    print foo_reduce('za4yxz')
    print foo_reduce('toxrf4')
    print foo_reduce('ifv1x3')
    print foo_map("/article/21/")
    print foo_map("/article/49/")
    print foo_map("/article/56/")
    '''