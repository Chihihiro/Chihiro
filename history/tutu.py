import pandas as pd
import numpy as np
from utils.database import config as cfg
from utils.database.models import base_private
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
from collections import OrderedDict
import re
import datetime as dt
from importlib import reload

engine_c = cfg.load_engine()["2Gcpri"]
engine_b = cfg.load_engine()["2Gb"]


def str2int(x):
    try:
        return int(x)
    except ValueError:
        return None


def remove_blank(x):
    try:
        return x.strip()
    except:
        return None


def stream_x_fund_info_account():
    inp = MysqlInput(engine_c, "\
    SELECT TB_MIN.version, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, reg_time_amac, fund_time_limit_amac, issuing_scale_amac, number_clients_amac \
    FROM x_fund_info_fundaccount TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_fundaccount WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id")

    vm = transform.ValueMap(
        {
            "fund_name_amac": lambda x: remove_blank(x),
            "reg_code_amac": lambda x: remove_blank(x),
            "fund_time_limit_amac": lambda x: str2int(x),
            "issuing_scale_amac": lambda x: x * 1e4 if x is not None else None,
            "number_clients_amac": lambda x: str2int(x),
        }
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "fund_id": "source_id",
            "reg_code_amac": "reg_code",
            "reg_time_amac": "reg_time",
            "fund_time_limit_amac": "limit_time",
            "issuing_scale_amac": "issuing_scale",
            "number_clients_amac": None,
            "fund_name_amac": "fund_full_name"
        }
    )

    stream = Stream(
        inp.frame, (vm, km)
    )

    return stream


def stream_x_fund_info_private():
    def clean_currency(x):
        patt = "(?P<currency>人民币|美元|多币种|欧元|港元|澳元|其他|其它|日元|英镑)?(?P<type>现钞|现汇)?"
        cpatt = re.compile(patt)
        sre = cpatt.search(x)
        if sre is not None:
            return sre.groupdict()
        return {}

    inp = MysqlInput(engine_c, "\
    SELECT TB_MAIN.version, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, reg_time_amac, foundation_date_amac, currency_name_amac, \
    fund_status_amac, final_report_time_amac \
    FROM x_fund_info_private TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_private WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id")

    vm = transform.ValueMap(
        OrderedDict(
            [
                ("fund_name_amac", lambda x: remove_blank(x)),
                ("reg_code_amac", lambda x: remove_blank(x)),
                ("is_abnormal_liquidation",
                 (lambda x: {"延期清算": 1, "提前清算": 1, "投顾协议已终止": 1, "正常清算": 0}.get(x), "fund_status_amac")),
                ("liquidation_cause",
                 (lambda x: {"延期清算": "延期清算", "提前清算": "提前清算", "投顾协议已终止": "投顾协议已终止"}.get(x), "fund_status_amac")),
                ("fund_status_amac", {"正常清算": "终止", "正在运作": "运行中", "延期清算": "运行中", "提前清算": "终止", "投顾协议已终止": "终止"}),
                ("currency_type", (lambda x: clean_currency(x).get("type"), "currency_name_amac")),
                ("currency_name_amac", lambda x: clean_currency(x).get("currency")),
            ]
        )
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "fund_id": "source_id",
            "fund_name_amac": "fund_full_name",
            "reg_code_amac": "reg_code",
            "reg_time_amac": "reg_time",
            "foundation_date_amac": None,
            "currency_name_amac": "currency",
            "currency_type": None,
            "fund_status_amac": "fund_status",
            "is_abnormal_liquidation": None,
            "liquidation_cause": None
        }
    )

    stream = Stream(inp.frame, (vm, km))
    return stream


def stream_x_fund_info_security():
    def clean_date(x):

        try:
            return dt.datetime.strptime(x, "%Y-%m-%d").date()

        except:
            return None

    inp = MysqlInput(engine_c, "\
    SELECT TB_MAIN.version, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, foundation_date_amac, fund_time_limit_amac \
    FROM x_fund_info_securities TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_securities WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id")

    vm = transform.ValueMap(
        OrderedDict(
            [
                ("fund_name_amac", lambda x: remove_blank(x)),
                ("reg_code_amac", lambda x: remove_blank(x)),
                ("limit_date", (lambda x: clean_date(x), "fund_time_limit_amac")),
                ("fund_time_limit_amac", lambda x: x if x == "无期限" else None)
            ]
        )
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "fund_id": "source_id",
            "fund_name_amac": "fund_full_name",
            "reg_code_amac": "reg_code",
            "foundation_date_amac": "foundation_date",
            "fund_time_limit_amac": "limit_time",
            "limit_date": "limit_date",
            "fund_status_amac": "fund_status",
        }
    )

    stream = Stream(inp.frame, (vm, km))
    return stream


def stream_x_fund_info_future():
    inp = MysqlInput(engine_c, "\
    SELECT TB_MAIN.version, TB_MAIN.fund_id, fund_name_amac, reg_code_amac, foundation_date_amac, issuing_scale_amac, number_clients_amac \
    FROM x_fund_info_futures TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver FROM x_fund_info_futures WHERE is_used = 1 GROUP BY fund_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id")

    vm = transform.ValueMap(
        OrderedDict(
            [
                ("fund_name_amac", lambda x: remove_blank(x)),
                ("reg_code_amac", lambda x: remove_blank(x)),
                ("number_clients_amac", lambda x: str2int(x)),
            ]
        )
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "fund_id": "source_id",
            "fund_name_amac": "fund_full_name",
            "reg_code_amac": "reg_code",
            "foundation_date_amac": "foundation_date",
            "issuing_scale_amac": "issuing_scale",
            "number_clients_amac": "number_clients",
        }
    )

    stream = Stream(inp.frame, (vm, km))
    return stream


def stream_d_fund_info_020001():
    def clean_regcode(x):
        patt = "\D\w\d{4}"
        sre = re.search(patt, x)
        if sre is not None:
            return sre.group()
        return None

    def clean_locktime(x):
        patt = "\D\w\d{4}"
        sre = re.search(patt, x)
        if sre is not None:
            return sre.group()
        return None

    inp = MysqlInput(engine_c, "\
    SELECT TB_MAIN.version, TB_MAIN.fund_id, TB_MAIN.source_id, fund_name, fund_full_name, reg_code, fund_status, locked_time_limit \
    open_date, \
    FROM d_fund_info TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver, source_id FROM d_fund_info WHERE source_id = '020001' and is_used = 1 GROUP BY fund_id, source_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id AND TB_MAIN.source_id = TB_LATEST.source_id")

    vm = transform.ValueMap(
        {
            "fund_name": lambda x: remove_blank(x),
            "fund_full_name": lambda x: remove_blank(x),
            "reg_code": lambda x: lambda x: clean_regcode(x),
            "locked_time_limit": lambda x: transform.CleanWrong("-|--"),
            "fund_status": {"正常": "运行中", "终止": "终止"}
        }
    )

    km = transform.MapSelectKeys(
        {
            # "version": None,
            "fund_id": "source_id",
            "fund_name": None,
            "fund_full_name": None,
            "reg_code": None,
            "fund_status": None,
            "issuing_scale_amac": "issuing_scale",
            "number_clients_amac": "number_clients",
        }
    )

    stream = Stream(inp.frame, ())
    return stream


def stream_d_fund_info_020002():
    inp = MysqlInput(engine_c, "\
    SELECT TB_MAIN.version, TB_MAIN.fund_id, TB_MAIN.source_id, fund_name, fund_full_name, fund_status, locked_time_limit, \
    open_date\
    FROM d_fund_info TB_MAIN \
    JOIN (SELECT fund_id, MAX(version) latest_ver, source_id FROM d_fund_info WHERE source_id = '020002' and is_used = 1 GROUP BY fund_id, source_id) as TB_LATEST \
    ON TB_MAIN.version = TB_LATEST.latest_ver AND TB_MAIN.fund_id = TB_LATEST.fund_id AND TB_MAIN.source_id = TB_LATEST.source_id")

    # vm = transform.ValueMap(
    #     OrderedDict(
    #         [
    #             ("fund_name_amac", lambda x: remove_blank(x)),
    #             ("reg_code_amac", lambda x: remove_blank(x)),
    #             ("number_clients_amac", lambda x: str2int(x)),
    #         ]
    #     )
    # )
    #
    # km = transform.MapSelectKeys(
    #     {
    #         "version": None,
    #         "fund_id": "source_id",
    #         "fund_name_amac": "fund_full_name",
    #         "reg_code_amac": "reg_code",
    #         "foundation_date_amac": "foundation_date",
    #         "issuing_scale_amac": "issuing_scale",
    #         "number_clients_amac": "number_clients",
    #     }
    # )
    #
    stream = Stream(inp.frame, ())
    return stream


class Confluence:
    def __init__(self, **streams):
        self._streams = streams


s1 = stream_x_fund_info_account()
s1.flow()

s2 = stream_x_fund_info_private()
s2.flow()

s3 = stream_x_fund_info_security()
s3.flow()

s4 = stream_x_fund_info_future()
s4.flow()

im = MysqlInput(engine_b, "\
SELECT matched_id, source_id FROM id_match WHERE id_type = 1 AND source = '010001' AND is_used = 1")
im.frame

j = transform.Join(on="source_id")
j.process(s1.frame, im.frame)
j.process(s2.frame, im.frame)
j.process(s3.frame, im.frame)
j.process(s4.frame, im.frame)
