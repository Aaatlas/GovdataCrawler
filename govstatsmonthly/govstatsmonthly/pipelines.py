# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql

class MysqlPipeline(object):
    def __init__(self,host,port,user,db,password):
        self.mysql=pymysql.connect(host=host,port=port,user=user,password=password,db=db)
        
    @classmethod
    def from_crawler(cls,crawler):
        return cls(
            host=crawler.settings['MYSQL'].get('host'),
            port=crawler.settings['MYSQL'].get('port'),
            user=crawler.settings['MYSQL'].get('user'),
            password=crawler.settings['MYSQL'].get('password'),
            db=crawler.settings['MYSQL'].get('db')
            )
    def open_spider(self,spider):
        self.mysql.cursor()
        sql="CREATE TABLE IF NOT EXISTS govdata (nid INT NOT NULL PRIMARY KEY AUTO_INCREMENT,quota VARCHAR(128) , tableid VARCHAR(32) , dt VARCHAR(32) , data FLOAT)"

        self.mysql.cursor().execute(sql)

    def process_item(self, item, spider):
        category=item['category']
        for id1,quota in item['tables']:
            for id2,date,data in item['data']:
                if id1 == id2:
                    #sql='insert into govdata(index,tableid,date,data) values(%s,%s,%s,%s)'
                    sqldata={'quota':quota,'tableid':id2,'dt':date,'data':data}
                    keys=','.join(sqldata.keys())
                    values=','.join(['%s']*len(sqldata))
                    sql='INSERT INTO govdata({keys}) VALUES ({values})'.format(keys=keys,values=values)
                    print(sql)
                    try:
                        print(quota,id2,date,data)
                        self.mysql.cursor().execute(sql,tuple(sqldata.values()))
                        self.mysql.commit()
                    except:
                        print("--rollback--")
                        self.mysql.rollback()
        return item

    def close_spider(self,spider):
        self.mysql.close()
