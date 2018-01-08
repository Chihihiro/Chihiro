# # # -*- coding: utf-8 -*-
# #
# # # Define your item pipelines here
# # #
# # # Don't forget to add your pipeline to the ITEM_PIPELINES setting
# # # See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
# # import sys
# #
# # sys.path.append('/home/michel/PycharmProjects/house_price')
# # sys.path.append('/home/michel/PycharmProjects/fangjia')
# # sys.path.append('/code/house_price')
# #
# # from sqlalchemy import create_engine
# # from fangjia.settings import DATABASES
# # import pandas as pd
# # from utils.io import to_sql
# # import datetime as dt
# # from .items import *
#
#
class FangjiaSpidersPipeline(object):
    def open_spider(self, spider):
        self.db_engine = create_engine(
            "mysql+pymysql://{}:{}@{}:{}/{}".format(DATABASES['default']['USER'], DATABASES['default']['PASSWORD'],
                                                    DATABASES['default']['HOST'], DATABASES['default']['PORT'],
                                                    DATABASES['default']['NAME'], ), connect_args={"charset": "utf8"})
        name_id = pd.read_sql('select xiaoqu_name, id from xiaoqu_info', self.db_engine)
        self.xiaoqu_name = dict(zip(name_id['xiaoqu_name'], name_id['id']))

    def process_item(self, item, spider):
        df = pd.DataFrame.from_dict([item])
        if isinstance(item, XiaoquItem):
            if df['aver_price'][0] is not None:
                if df['xiaoqu_name'][0] in self.xiaoqu_name.keys():
                    df['id'] = self.xiaoqu_name[df['xiaoqu_name'][0]]
                else:
                    new_xiaoqu = df[['xiaoqu_name', 'id']]
#                     self.xiaoqu_name[new_xiaoqu['xiaoqu_name'][0]] = new_xiaoqu['id'][0]
#                     to_sql('xiaoqu_info', self.db_engine, new_xiaoqu, type='update')
#                 df['static_date'] = dt.date.today()
#                 to_sql('xiaoqu_fangjia', self.db_engine, df)
#         elif isinstance(item, HouseItem):
#             if df['house_id'][0] is not None:
#                 df['static_date'] = dt.date.today()
#                 to_sql('house_fangjia', self.db_engine, df)
#                 xiaoqu_house = df[['xiaoqu_name', 'house_id']]
#                 xiaoqu_house['id'] = self.xiaoqu_name[df['xiaoqu_name'][0]]
#                 to_sql('xiaoqu_house_mapping', self.db_engine, xiaoqu_house, type='update')
#         elif isinstance(item, XiaoquLocaItem):
#             if df['location'][0] is not None:
#                 to_sql('xiaoqu_info', self.db_engine, df, type='update')
#         return item
#
#     def spider_close(self, spider):
# pass