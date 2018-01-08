from io import to_sql
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 创建对象的基类:
test= declarative_base()

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
DBSession = sessionmaker(bind=engine)

#
# 定义User对象:
class t_doubantop(test):
    # 表的名字:
    __tablename__ = 't_doubantop'

    # 表的结构:
    num = Column(String(50), primary_key=True)
    name = Column(String(50))
    charactor = Column(String(50))
    remark = Column(String(50))
    score = Column(String(50))


    def __init__(self, num ,name,charactor,remark,score):
        self.num = num
        self.name = name
        self.charactor = charactor
        self.remark=remark
        self.score=score


    def __repr__(self):
        return "<t_doubantop('%s','%s', '%s','%s','%s')>" % (self.num, self.name, self.charactor,self.remark,self.score)



session = DBSession()
# #
# v=[t_doubantop( 253,'戴白', '小白','三百','疾风买买提'),t_doubantop( 254,'戴白', '小白','三百','疾风买买提')]
# v1=t_doubantop( 254,'戴白', '小白','三百','疾风买买提')
# v2=t_doubantop( 255,'戴白', '小白','三百','疾风买买提')
#
#
#
# session.add(v2)
#
# # 提交即保存到数据库:
# session.commit()
# # 关闭session:
# session.close()
#



# # 创建session对象:
# session = DBSession()
# # 创建新User对象:
# new_user =t_doubantop(num=251,name='aaronlau',charactor= '123456789',remark='dasd',score='dasdasd')
# # 添加到session:
# session.add(new_user)
# # 提交即保存到数据库:
# session.commit()
# # 关闭session:
# session.close()


# session.execute('select * from t_doubantop')
# # conn.execute("SELECT fund_id,fund_name,fund_full_name FROM fund_info WHERE fund_id like 'JR00000%'")
#
# rows = session.fetchall()  # fetchall(): 接收全部的返回结果行，若没有则返回的是表的内容个数 int型
# info=[]
# for i in rows:
#     # v =str(i)
#     info.append(i[0])
#
# # print(i[1])
# print(info)

# session.execute('create database abc')
print(session.execute('select * from t_doubantop').fetchall())
# session.execute('use abc')


