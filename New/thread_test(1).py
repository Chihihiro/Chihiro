from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
import pandas as pd
from utils.database import config as cfg
from functools import partial


engine = cfg.load_engine()["2Gb"]


def fetch_nv(fids):
    fids = str(tuple(fids))
    sql = "SELECT fund_id, statistic_date, nav, added_nav FROM fund_nv_data_standard " \
          "WHERE fund_id IN {fids}".format(
        fids=fids
    )
    return pd.read_sql(sql, engine)


def main():
    all_ids = ["JR" + "0" * (6 - len(str(i))) + str(i) for i in range(100)]
    STEP = 10
    sliced = [all_ids[i: i+STEP] for i in range(0, len(all_ids), STEP)]



    pool = ThreadPool(6)
    p2 = Pool(2)

    result = pool.map(fetch_nv, sliced)


if __name__ == "__main__":
    main()
