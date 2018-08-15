from engine import *


def main():
    sql = "SELECT idm.matched_id,oi.org_id,oi.org_full_name FROM \
            (SELECT DISTINCT matched_id,source FROM base.id_match  \
            where id_type = 1 and is_used = 1 AND source like '040%%' \
            AND matched_id NOT in \
            (select fund_id from base.fund_org_mapping where org_type_code=2)) AS idm \
            JOIN \
            data_test.source_info_org as sig \
            ON idm.source = sig.source_id \
            JOIN base.org_info as oi \
            ON sig.org_id = oi.org_id"

    df = pd.read_sql(sql, engine_base)
    df["org_type"] = "基金管理人"
    df["org_type_code"] = 2
    df.rename(columns={"matched_id": "fund_id", "org_full_name": "org_name"}, inplace=True)
    to_sql("fund_org_mapping", engine_base, df, type="update")  # ignore


if __name__ == '__main__':
    main()
