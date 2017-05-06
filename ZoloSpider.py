

import re
import urllib2  
import sqlite3
import random
import threading
from bs4 import BeautifulSoup
import os.path

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]
    

#cities in GTA
regions=[u"Markham"]


lock = threading.Lock()


class SQLiteWraper(object):
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock()
        self.path = path
        
        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)
    
    def get_conn(self):  
        conn = sqlite3.connect(self.path)#,check_same_thread=False)  
        conn.text_factory=str
        return conn   
      
    def conn_close(self,conn=None):  
        conn.close()  
    
    def conn_trans(func):  
        def connection(self,*args,**kwargs):  
            self.lock.acquire()  
            conn = self.get_conn()  
            kwargs['conn'] = conn  
            rs = func(self,*args,**kwargs)  
            self.conn_close(conn)  
            self.lock.release()  
            return rs  
        return connection  
    
    @conn_trans    
    def execute(self,command,method_flag=0,conn=None):  
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError,e:
            #print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0
    
    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists

def gen_region_insert_command(info_dict):
    info_list=[u'url']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?)",t)
    return command


def gen_house_insert_command(info_dict):
    info_list=[u'url',u'address',u'region',u'community',u'price',u'bedrooms',u'bathrooms',u'tax',u'maintainance',u'type',u'style',u'size',u'age',u'walkscore']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def region_spider(db_xq,url_page=u"https://www.zolo.ca/markham-real-estate/page-2"):
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text,"html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)
    
    house_list=soup.findAll('div',{'class':'listing-column'})
    for house in house_list:
        info_dict={}
        info_dict.update({u'url':xq.find('a').href})
        command=gen_region_insert_command(info_dict)
        db_xq.execute(command,1)

    
def do_region_spider(db_xq,region=u"markham"):
    url=u"https://www.zolo.ca/"+region+"-real-estate"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=5).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,"html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return

    print "region:"+str(soup)
    total_pages_section=soup.find('section',{'class':'supplementary-nav'}).findAll('a')
    print "total_pages_section = "+str(total_pages_section)
    
    threads=[]
    for i in range(int(d.text)):
        url_page=url+u"/page-%d/" % (i+1)
        t=threading.Thread(target=region_spider,args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print u"got all information for reagion %s" % region


def house_spider(db_cj,url_page=u"https://www.zolo.ca/markham-real-estate/27-timbermill-crescent"):
    try:
        #print "chengjiao spider url = "+url_page
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,"html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('house_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('house_spider',url_page)
        return
    
    info_dict={}
    info_dict.update({u'url':url_page})

    address = soup.find('h1',{'class':'address'}).text
    info_dict.update({u'address':address})

    area = soup.find('div',{'class':'area'})
    city = area.findAll('a')[0].text
    community = area.findAll('a')[1].text
    info_dict.update({u'region':city})
    info_dict.update({u'community':community})

    listing_price = soup.find('div',{'class':'listing-price-value'})
    info_dict.update({u'price':listing_price})

    bedrooms = soup.find('div',{'class':'listing-values-bedrooms'}).text
    bathrooms = soup.find('div',{'class':'listing-values-bathrooms'}).text
    info_dict.update({u'bedrooms':bedrooms})
    info_dict.update({u'bathrooms':bathrooms})

    listing_content = soup.find('div',{'class':'section-listing-content'})
    columns = listing_content.findAll('dd',{'class':'column-value'})

    info_dict.update({u'tax':columns[1]})
    info_dict.update({u'maintainance':columns[2]})
    info_dict.update({u'type':columns[3]})
    info_dict.update({u'style':columns[4]})
    info_dict.update({u'size':content[5]})
    info_dict.update({u'age':content[6]})
    info_dict.update({u'walkscore':content[7]})
    command=gen_house_insert_command(info_dict)
    db_cj.execute(command,1)



def do_house_spider(db_xq,db_cj):
    count=0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        house_spider(db_cj,xq[0])
        count+=1
        print 'have spidered %d house' % count
    print 'done'


def exception_write(fun_name,url):
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    lock.acquire()
    if(os.path.exists('log.txt')):
        f=open('log.txt','r')
        lines=f.readlines()
        f.close()
        f=open('log.txt','w')
        f.truncate()
        f.close()
        lock.release()
        return lines


def exception_spider(db_cj):
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="region_spider":
                region_spider(db_cj,url)
                count+=1
            elif excep_name=="house_spider":
                house_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'
    


if __name__=="__main__":
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, url TEXT)"
    db_xq=SQLiteWraper('lianjia-xq.db',command)
    
    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT)"
    db_cj=SQLiteWraper('lianjia-cj.db',command)
    
    for region in regions:
        do_region_spider(db_xq,region)
    
    do_house_spider(db_xq,db_cj)
    
    #exception_spider(db_cj)

