# -*- coding: utf-8 -*-
import scrapy
import json
from urllib.parse import quote
from govstatsmonthly.items import GovstatsmonthlyItem

class GovdataSpider(scrapy.Spider):
    name = 'govdata'
    allowed_domains = ['data.stats.gov.cn']
    #start_urls = ['http://data.stats.gov.cn/']
    
    def init(self):
        #self.dirname=""
        #self.table=""
        super(scrapy.Spider,self).__init__()

    def start_requests(self):
        start_url="http://data.stats.gov.cn/easyquery.htm"
        yield scrapy.FormRequest(start_url,formdata={'dbcode':'hgyd','id':'zb','m':"getTree",'wdcode':'zb'},callback=self.parse,meta={})

    def parse(self,response):
        print(" parse response from post ... ")
        data=json.loads(response.text)
        if response.meta.get('name'):
            dirname=response.meta.get('name')
        else:
            dirname=""

        for item in data:
            if item.get('isParent'):
                id=item.get('id')
                name=item.get('name')
                name=dirname+'-'+name
                yield self.post_requests(id,name)

            elif item.get('isParent')==False:
                id=item.get('id')
                name=item.get('name')
                name=dirname+'-'+name
                yield self.get_requests(id,name)

            else:
                print("???data????",)
                print(data)
            
    def post_requests(self,id,name):
        print(' post_requests***')
        url="http://data.stats.gov.cn/easyquery.htm"
        return scrapy.FormRequest(url,formdata={'dbcode':'hgyd','id':id,'m':'getTree','wdcode':'zb'},callback=self.parse,meta={"name":name})
        
    def get_requests(self,id,name):
        print(" get_requests...")
        url="http://data.stats.gov.cn/easyquery.htm?"
        params='m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"%s":"%s","%s":"%s"}]'%(quote('wdcode'),quote('zb'),quote('valuecode'),quote(id))
        print(url+params)
        url=url+params
        return scrapy.Request(url,callback=self.target_parse,meta={"name":name})

    def target_parse(self,response):
        print("last request ... ")
        print("collecting data...")

        itemobj=GovstatsmonthlyItem()
        tablelist=[]
        datalist=[]
        itemobj['category']=response.meta.get('name').lstrip('-')
        
        data=json.loads(response.text)
        wdnodes=data.get("returndata").get("wdnodes")
        for item in wdnodes[0].get('nodes'):
            table=item.get('cname')
            id=item.get('code')
            tablelist.append((id,table))
        
        datanodes=data.get('returndata').get('datanodes')
        for tem in datanodes:
            data=tem.get('data').get('data')
            id=tem.get('wds')[0].get('valuecode')
            date=tem.get('wds')[1].get('valuecode')
            datalist.append((id,date,data))
        
        itemobj['tables']=tablelist
        itemobj['data']=datalist
        print("return itemobj...")
        return itemobj
            
