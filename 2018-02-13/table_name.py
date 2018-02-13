from sqlalchemy import create_engine
import numpy as np
import pandas as pd


engine_base = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'base', ), connect_args={"charset": "utf8"}, echo=True, )
# engine_base = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)


def table_name():
    cc = pd.read_sql("show tables", engine_base)
    bb = np.array(cc)#np.ndarray()
    vv=bb.tolist()#list
    # vv.remove(["fund_info"],["fund_nv_data_source_copy2"],["fund_subsidiary_month_index"],["fund_subsidiary_month_index2"],["fund_subsidiary_month_index3"]
    #           ,["fund_subsidiary_weekly_index"],["fund_subsidiary_weekly_index2"],["fund_subsidiary_weekly_index3"],
    #           ["fund_weekly_return"],["fund_weekly_risk"],["fund_weekly_risk2"])
    # vv.remove(["fund_nv_data_source_copy2"])
    vv.remove(["fund_info"])
    vv.remove(["fund_nv_data_source"])

    print(vv)

    tablenames=[]
    for q in vv:
        str = "".join(q)
        pp=pd.read_sql("show columns from {}".format(str),engine_base)
        li=pp["Field"]
        col = np.array(li)
        print(li)
        x="fund_name"
        y="fund_full_name"
        table=[]
        if y in col:
            table.append(str)
            table.append(y)
        elif x in col:
            table.append(str)
            table.append(x)
        else:
            print("米有")
        tablenames.append(table)
    all=[]
    for i in tablenames:
        if i == []:
            pass
        else:
            all.append(i)
    return all



def crawl():
    table=table_name()
    print(table)
    for i in table:
        tab=i[0]
        p=i[1]
        x="fund_name"
        y="fund_full_name"
        if p==x:
            engine_base.execute("UPDATE {} fi JOIN fund_info fo ON fi.fund_id = fo.fund_id \
            SET fi.fund_name = fo.fund_name".format(tab))
        elif p==y:
            engine_base.execute ("UPDATE {} fi JOIN fund_info fo ON fi.fund_id = fo.fund_id \
            SET fi.fund_name = fo.fund_name,fi.fund_full_name=fo.fund_full_name".format(tab))
        else:
            print("????")


def crawl2():
    engine_base.execute("update manager_info as a JOIN org_info as b on a.org_id=b.org_id set a.org_name=b.org_name")
    engine_base.execute("update fund_org_mapping AS a JOIN org_info as b on a.org_id=b.org_id set a.org_name=b.org_name;")


def main():
    crawl()
    crawl2()

if __name__ == "__main__":
    main()





































# table1=pd.read_sql("select fund_id,fund_name from fund_info",engine_base)
# dict ={key:value for key,value in zip(table1["fund_id"],table1["fund_name"])}
#
# table2=pd.read_sql("select fund_id,fund_full_name from fund_info",engine_base)
# dict2 ={key:value for key,value in zip(table2["fund_id"],table2["fund_full_name"])}
#
# def sql(fund_table,fund_name,fund_id):
#     engine_base.execute("UPDATE {} set fund_name='{}' WHERE fund_id = '{}'".format(fund_table,fund_name,fund_id))
#
# def sql(fund_table,fund_name,fund_full_name,fund_id):
#     engine_base.execute("UPDATE {} set fund_name='{}',fund_full_name='{}' WHERE fund_id = '{}'".format(fund_table,fund_name,fund_full_name,fund_id))

