import pandas as pd
from sqlalchemy import create_engine


engine_base = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )



df=pd.read_sql("SELECT fund_id,fund_full_name FROM fund_info WHERE fund_id NOT IN (SELECT fund_id FROM fund_type_mapping WHERE typestandard_code = 604)",engine_base)


dict={
60401:'信托',
60402:'自主发行',
60403:'公募专户及子公司资管计划',
60404:'券商资管',
60405:'期货资管',
60406:'保险公司及子公司资管计划',
60407:'海外基金',
60408:'有限合伙',
60409:'单账户',
60410:'其他'}
dict2={
    "信托":60401,
    "集合资产管理":60402,





}
