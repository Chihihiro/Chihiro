from engine import *

typestandard_code_dict = {
    '按全产品分类': 600,
    '按投资策略分类': 601,
    '按结构类型分类': 602,
    '按投资标的分类': 603,
    '按发行主体分类': 604,
    '按基金类型分类': 605,
}


def datama(fund_id):
    sql = "SELECT a.fund_id,ft.typestandard_name, \
            ft.type_code,ft.type_name,ft.stype_code,ft.stype_name,  \
            ft.confirmed,ft.classified_by, ft.typestandard_code FROM ( \
            SELECT MAX(update_time) mm,typestandard_code,fund_id  \
            FROM base.fund_type_mapping_import WHERE fund_id = '{fid}' and typestandard_code LIKE '602' GROUP BY typestandard_code) as a  \
            JOIN base.fund_type_mapping_import as ft  \
            ON ft.fund_id = a.fund_id  \
            AND a.typestandard_code = ft.typestandard_code  \
            and a.mm = ft.update_time".format(fid=fund_id)
    df = pd.read_sql(sql, engine_base)
    # df["typestandard_code"] = df["typestandard_name"].apply(lambda x: typestandard_code_dict.get(x))
    return df


ids = ['JR088784',
       'JR157386',
       'JR157441',
       'JR157461',
       'JR091689',
       'JR082558',
       'JR082560',
       'JR082564',
       'JR082928',
       'JR082661',
       'JR082663',
       'JR082682',
       'JR082690',
       'JR082692',
       'JR082696',
       'JR082694',
       'JR082851',
       'JR082803',
       'JR082859',
       'JR099864',
       'JR000322',
       'JR089443',
       'JR137860',
       'JR083089',
       'JR083082',
       'JR081657',
       'JR087811',
       'JR108019',
       'JR033951',
       'JR083196',
       'JR000460',
       'JR090918',
       'JR087808',
       'JR011980',
       'JR087867',
       'JR067914',
       'JR087337',
       'JR096574',
       'JR157370',
       'JR157375',
       'JR147133']

for i in ids:
    df = datama(i)
    to_sql("fund_type_mapping_import", engine_base, df, type="update")