import os
import pandas as pd
import shutil
from History.iosjk import to_sql
from sqlalchemy import create_engine

#### SETTING HERE ####
DEFAULT_DIR = "c:/Users/63220/Desktop/TEST"
MODE = "STRICT"
COLS_USED = {"version","org_id","org_name","org_full_name","org_web","profile	phone","email	team","investment_idea"}
ENGINE_WT = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'crawl_private', ), connect_args={"charset": "utf8"}, echo=True, )
######################




def init_dir(root_dir):
    if os.path.isdir(root_dir):
        DIR_CHECKED = os.path.join(root_dir, "checked")
        DIR_ERRORS = os.path.join(root_dir, "errors")

        for d in {DIR_CHECKED, DIR_ERRORS}:
            if os.path.isdir(d) is False:
                print("New Directory: ", d)
                os.mkdir(d)
    else:
        raise FileNotFoundError("{directory} is not existed".format(directory=root_dir))


def read_data(file):
    file_suffix = os.path.splitext(file)[-1]
    if file_suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(file)
    elif file_suffix in {".csv"}:
        df = pd.read_csv(file)
    else:
        return



    return df

def main():
    init_dir(DIR)

    files = list(os.walk(DIR))[0][2]
    err_log = {}

    for file in files:
        try:
            whole_file_path = os.path.join(DIR, file)
            if file == "daoru2.py": continue

            df = read_data(whole_file_path)

            if MODE != "STRICT":
                to_sql("g_fund_nv", ENGINE_WT, df)
                shutil.move(whole_file_path, os.path.join(DIR, "checked", file))
                err_log[file] = "Done"
            else:
                print("FILE NAME:", file)
                print(df)
                inp = input("input 1 to write to db...\n")
                if inp == "1":
                    to_sql("y_org_info", ENGINE_WT, df)
                    shutil.move(whole_file_path, os.path.join(DIR, "checked", file))
                    err_log[file] = "Done"
                else:
                    err_log[file] = "Unchecned"
                continue
        except Exception as e:
            err_log[file] = "ERROR: " + str(e)
            shutil.move(whole_file_path, os.path.join(DIR, "errors", file))

    print(err_log)


if __name__ == "__main__":
    # DIR = input("INPUT YOUR ROOT DIRECTORY\n") or DEFAULT_DIR
    DIR = DEFAULT_DIR
    main()
    input("press any key to exit...")