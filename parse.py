import copy
from lxml import etree
import requests
import gc
import pdb 
header={"User-Agent":
'''Mozilla/5.0 (Windows NT 5.1) 
AppleWebKit/537.36 (KHTML, like Gecko) 
Chrome/30.0.1599.101 Safari/537.36'''}

#url='http://www.guokr.com/post/483103/'

post_parttern="//ul[@class = 'cmts-list']//div[@class='pt-txt']//@data-ukey"
article_parttern ="//ul[@class = 'cmts-list ']//div[@class='pt-txt']//@data-ukey"

#@profile
def _ukeys_gen(post,pattern ='article'):
    paterns={
            'post':"//ul[@class='cmts-list']//div[@class='pt-txt']//@data-ukey",
            'article':"//ul[@class='cmts-list 'or'cmts-list  cmts-hide  ']//div[@class='pt-txt']//@data-ukey"
            }
    def next_page(page_lst):
        for link in page_lst :
            if link not in page_cache:
                page_cache[link]=0
            elif page_cache[link]=='checked':continue
            else :return link
        else: return None

    url = "http://www.guokr.com/%s" %(post)
    page_cache={}
    page_cache[post]='checked'
    while True:    
        r = requests.get(url,headers=header)
        dom = etree.HTML(r.content.decode("utf-8"))
        ukey_lst=dom.xpath(paterns[pattern])
        page_lst= dom.xpath("//ul[@class = 'gpages']//@href")

        next_link=next_page(page_lst)

        yield [str(x) for x in ukey_lst]
        
        #del(r)
        #del(dom)

        if  next_link:#update the next page            
            url = "http://www.guokr.com/%s" %(next_link)
            page_cache[next_link]='checked'
        else:
            #yield ukey_lst
            break
    #gc.collect()
   

#@profile
def get_ukeys(pid,parttern='article'):
    gen = _ukeys_gen(pid,parttern)
    ul = tuple(gen)
    #pdb.set_trace()
    gen.close()
    #del(gen)
    
    ret = sum(ul,[])

    gc.collect()
    return set(ret)


if __name__ == '__main__':
    #u_lst= ukeys("/post/551514/")
    #for l in u_lst:
    #   print l,len(l)
    #uk1 = get_ukeys('/post/551514/','post')
    uk2 = get_ukeys('/article/437423/','article')
    #print uk1 ,len(uk1)
    
    print uk2 ,len(uk2)
