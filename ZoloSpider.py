

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
    

#上海区域列表
regions=[u"Markham"]


lock = threading.Lock()


class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock() #锁  
        self.path = path #数据库连接参数  
        
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


def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    #info_list=[u'小区链接',u'大区域',u'小区域',u'小区户型',u'建造时间']
    info_list=[u'小区链接']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?)",t)
    return command


def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'链接',u'小区名称',u'户型',u'面积',u'朝向',u'楼层',u'建造时间',u'签约时间',u'签约单价',u'签约总价',u'房产类型',u'学区',u'地铁']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def xiaoqu_spider(db_xq,url_page=u"https://www.zolo.ca/markham-real-estate/aileen-willowbrook"):
    """
    爬取页面链接中的小区信息
    """
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
    
    xiaoqu_list=soup.findAll('div',{'class':'info-panel'})
    for xq in xiaoqu_list:
        info_dict={}
        info_dict.update({u'小区链接':url_page})
        #print "小区名称="+xq.find('a').text
        #content=unicode(xq.find('div',{'class':'con'}).renderContents().strip())
        #info=re.match(r".+>(.+)</a>.+>(.+)</a>.+</span>(.+)<span>.+</span>(.+)",content)
        #if info:
        #    info=info.groups()
        #    info_dict.update({u'大区域':info[0]})
        #    info_dict.update({u'小区域':info[1]})
        #    info_dict.update({u'小区户型':info[2]})
        #    info_dict.update({u'建造时间':info[3][:4]})
        command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)

    
def do_xiaoqu_spider(db_xq,region=u"markham"):
    """
    爬取大区域中的所有小区信息
    """
    url=u"https://www.zolo.ca/"+region+"-real-estate/neighbourhoods"
    #print "do xiaoqu spider:"+url
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
    #print "soup="+str(soup)
    neighbourhoods=soup.find('section',{'class':'all-neighborhoods xs-mb5'}).find('tbody').findAll('tr')

    
    threads=[]
    for neighbourhood in neighbourhoods:
        url_page = neighbourhoods.find('a').href
        t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print u"爬下了 %s 区全部的小区信息" % region


def single_house_spider(db_cj,url_page=u"https://www.zolo.ca/markham-real-estate/27-timbermill-crescent"):
    """
    爬取页面链接中的成交记录
    """
    try:
        #print "chengjiao spider url = "+url_page
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,"html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('single_house_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('single_house_spider',url_page)
        return
    
    info_dict={}
    info_dict.update({u'链接':url_page]})

    address = soup.find('h1',{'class':'address'}).text
    info_dict.update({u'地址':address]})

    area = soup.find('div',{'class':'area'})
    city = area.findAll('a')[0].text
    community = area.findAll('a')[1].text
    info_dict.update({u'城市':city})
    info_dict.update({u'小区':community})

    listing_price = soup.find('div',{'class':'listing-price-value'})
    info_dict.update({u'出价':listing_price})

    bedrooms = soup.find('div',{'class':'listing-values-bedrooms'}).text
    bathrooms = soup.find('div',{'class':'listing-values-bathrooms'}).text
    info_dict.update({u'卧室':bedrooms})
    info_dict.update({u'浴室':bathrooms})

    bedrooms = soup.find('div',{'class':'listing-values-bedrooms'}).text
    info_dict.update({u'户型':content[1]})
        info_dict.update({u'面积':content[2]})
        print "小区名称=%s,户型=%s,面积=%s" %(content[0],content[1],content[2])
    content=unicode(cj.find('div',{'class':'row1-text'}).text).strip().replace('\n', '')
    #print "row one text = "+content
    content=content.split('|')
    if (len(content)>0):
        floor=content[0].rstrip()
        info_dict.update({u'楼层':floor})
    else:
        floor=""
    if(len(content)>1):
        direction=content[1].rstrip()   
        info_dict.update({u'朝向':direction})
    else:
        direction=""
    print "朝向=%s,楼层=%s" %(direction,floor)
    sign_up_time=cj.find('div',{'class':'info-col deal-item main strong-num'}).text
    info_dict.update({u'签约时间':sign_up_time})
    total_price=cj.find('div',{'class':'info-col price-item main'}).text.rstrip().replace('\n', '')
    info_dict.update({u'签约总价':total_price})
    price_per_square_metre=cj.find('div',{'class':'info-col price-item minor'}).text.rstrip()
    info_dict.update({u'签约单价':price_per_square_metre})
    print "签约时间=%s,签约总价=%s,签约单价=%s" %(sign_up_time,total_price,price_per_square_metre)
    property_tag=cj.find('div',{'class':'property-tag-container'})
    if property_tag:
        properties = property_tag.findAll('span')
        for p in properties:
            print p.text
            if p.text.find(u'满')!=-1:
                info_dict.update({u'房产类型':p.text})
            elif p.text.find(u'学')!=-1:
                info_dict.update({u'学区':p.text})
            elif p.text.find(u'距')!=-1:
                info_dict.update({u'地铁':p.text})
    
    command=gen_chengjiao_insert_command(info_dict)

    db_cj.execute(command,1)


def xiaoqu_chengjiao_spider(db_cj,url=u"https://www.zolo.ca/markham-real-estate/german-mills"):
    """
    爬取小区成交记录
    """
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,"html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    content=soup.find('div',{'class':'c-pagination'})
    total_pages=0
    if content:
        #print "content="+str(content)
        total_page=content.find('span',{'class':'button button--mono button--large button--disabled rounded-none xs-mr05'}).text
        print "total page = "+str(total_pages)
    threads=[]
    for i in range(1..total_page):
        url_page=url+"/page-%d" % (i)
        t=threading.Thread(target=chengjiao_spider,args=(db_cj,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    
def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    """
    批量爬取小区成交记录
    """
    count=0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj,xq[0])
        count+=1
        print 'have spidered %d xiaoqu' % count
    print 'done'


def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
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
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="chengjiao_spider":
                chengjiao_spider(db_cj,url)
                count+=1
            elif excep_name=="xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'
    


if __name__=="__main__":
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, style TEXT, year TEXT)"
    db_xq=SQLiteWraper('lianjia-xq.db',command)
    
    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT)"
    db_cj=SQLiteWraper('lianjia-cj.db',command)
    
    #爬下所有的小区信息
    for region in regions:
        do_xiaoqu_spider(db_xq,region)
    
    #爬下所有小区里的成交信息
    do_xiaoqu_chengjiao_spider(db_xq,db_cj)
    
    #重新爬取爬取异常的链接
    exception_spider(db_cj)

