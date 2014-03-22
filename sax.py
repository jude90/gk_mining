from lxml import etree 
import json
fs = open('sitemap.xml')
#doc = etree.fromstring(fs.read())
urls=[]

context = etree.iterparse(fs)
for event , elem in context:
    #print("%s: %s" % (event, elem.tag))
    if elem.tag.endswith('loc'):
        urls.append(elem.text.replace('http://www.guokr.com','').encode('utf-8'))
    


json.dump(urls,open('out.txt','w+'),indent=4)
fs.close()