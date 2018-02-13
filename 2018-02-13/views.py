import xlrd
import pandas as pd
import numpy as np
from collections import OrderedDict
# from DateFunc import fn_timer
import wrapcache
import datetime as dt
import platform
import random
#
# from rest_framework import permissions
# from rest_framework.decorators import api_view, permission_classes
# from rest_framework.response import Response

if platform.system() == "Linux":
    _file_dir = '/usr/local/upload'
else:
    _file_dir = 'E:/Data/upload'

PLACEHOLDER = "-##---####"

XL_DATEMODE = 0

_TABLE_CHN = (
    '基本信息',
    '股东构成及股权结构',
    '公司组织结构',
    '近三年财务状况',
    '近三年管理规模变化情况',
    '近三年获奖情况',
    '团队规模及变动',
    '公司关键人员信息',
    '公司团队规划与激励机制',
    '公司主要投资策略\n（请按主次程度依序填写）',
    '各类策略说明',
    '产品收益及风险指标',
    '拟合作产品要素',
    '已成立产品要素信息（可选）',
    'IT系统架构',
    '风控',
    '补充说明',
)

_TABLE_ENG = (
    'org_info',
    'shareholder',
    'department',
    'operation_state',
    'asset_scale',
    'prize',
    'team_scale',
    'core_member',
    'team_plan',
    'main_strategy',
    'strategy_info',
    'fund_indicator',
    'cooperation_factor',
    'fund_info',
    'it',
    'rc',
    'remark'
)
TABLES = OrderedDict(zip(_TABLE_ENG, _TABLE_CHN))

TABLE_SIZE = {
    'org_info': (14, 4),
    'shareholder': ('*', 4),
    'department': ('*', 4),
    'operation_state': (7, 4),
    'asset_scale': (3, 4),
    'prize': ('*', 4),
    'team_scale': ('*', 5),
    'core_member': ('*', 9),
    'team_plan': (3, 2),
    # 'main_strategy',
    # 'strategy_info',
    # 'fund_indicator',
    # 'cooperation_factor',
    # 'fund_info',
    # 'it',
    # 'rc',
    # 'remark'
}

# TODO 2.字段命名称映射

ORG_INFO = {
    "机构ID"
    "机构名称",
    "公司投资策略",
    "成立时间",
    "登记时间",
    "注册地址",
    "办公地址",
    "基金业协会登记编号",
    "组织机构代码",
    "注册资本(万)",
    "实缴资本(万)",
    "是否具备投顾资格",
    "已发行产品数量",
    "截至上月末管理产品规模（亿）",
    "机构投资者占比",
    "个人投资者占比",
    "其他占比",
    "主要股东和实际控制人情况",
    "公司理念与公司文化",
    "公司经营规划",
    "联系人",
    "联系人职位",
    "联系人电话",
    "联系人邮箱",
    "投资理念",
    "投资流程",
    "补充说明",
    "公司团队整体规划",
    "人员考核方式（如有）",
    "公司激励机制（如有）",
}


@api_view(http_method_names=['POST', 'GET'])
@permission_classes((permissions.AllowAny,))
def receive_file(request):
    """
    文件上传：
    Path + upload/

    1). 上传尽调报告：
    Params = {'org_id':org_id, 'type':'reports', 'file':文件}

    2). 上传原始资料：
    Params = {'org_id':org_id, 'file':文件, 'type': 'source_material'}

    :param request:
    :return:
    """

    import os
    uploaded_file = request.data.get("file", None)
    org_id = request.data.get('org_id')
    key = request.data.get("type", 'reports')
    if key not in ('reports', 'source_material') or not uploaded_file:
        return Response({'succeed': False})
    org_dir = os.path.join(_file_dir, org_id)
    if not os.path.exists(org_dir):
        os.mkdir(org_dir)
    file_dir = os.path.join(org_dir, key)
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    file_name = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '_' + str(
        random.randint(1, 100)) + '_' + uploaded_file.name
    filefullpath = os.path.join(file_dir, file_name)
    with open(filefullpath, 'wb+') as saved_file:
        for chunk in uploaded_file.chunks():
            saved_file.write(chunk)
    return Response({'succeed': True})


@wrapcache.wrapcache(timeout=120)
def parse_table_location(path):
    _LOCATIONS = {}
    wb = xlrd.open_workbook(path)
    for sheet in wb.sheets():
        _sheet_location = OrderedDict()
        for row_num, row_cells in enumerate(sheet.get_rows()):
            row_data = _parse_rows(row_cells)
            if not len([x for x in row_data if x]):
                continue
            for tb_name_e, tb_name in TABLES.items():
                if tb_name in row_data:
                    _sheet_location.update({tb_name_e: row_num})
        if len(_sheet_location):
            _sheet_location.update({'end': row_num})
            _sheet_location = sort_ordered_dict(_sheet_location)
            _tb_name = list(_sheet_location.keys())[:-1]
            _tb_location = list(_sheet_location.values())
            for i, j in enumerate(_tb_name):
                _LOCATIONS.update(
                    {j: {
                        'start_nrow': _tb_location[i],
                        'end_nrow': _tb_location[i + 1],
                        'sheet_name': sheet.name,
                    }, }
                )
    return _LOCATIONS


# _TABLE_SHEET_MAP = {
#     '1.公司基本信息': ('基本信息', '股东构成及股权结构', '公司组织结构',
#                  '近三年财务状况', '近三年管理规模变化情况', '近三年获奖情况'),
#     '2.核心团队人员': ('团队规模及变动', '公司关键人员信息', '公司团队规划与激励机制'),
#     '3.投资策略': ('公司主要投资策略\n（请按主次程度依序填写）', '各类策略说明', '产品收益及风险指标'),
#     '4.产品要素': ('拟合作产品要素', '已成立产品要素信息（可选）'),
#     '5.IT与风控': ('IT系统架构', '风控'),
#     '6.补充说明': ('补充说明',),
# }
# def parse_location(path):
#     wb = xlrd.open_workbook(path)
#     _TABLE_LOCATION = {}
#     for sheet_name, tables in _TABLE_SHEET_MAP.items():
#         sheet = wb.sheet_by_name(sheet_name)
#         _LOCATIONS = OrderedDict()
#         for row_num, row_cells in enumerate(sheet.get_rows()):
#             row_data = _parse_rows(row_cells)
#             if not len([x for x in row_data if x]):
#                 continue
#             for tb_name in tables:
#                 if tb_name in row_data:
#                     _LOCATIONS.update({tb_name: row_num})
#         _LOCATIONS.update({'end': row_num})
#         _TABLE_LOCATION.update({sheet_name: _LOCATIONS})
#     return _TABLE_LOCATION

#
# def parse_basic_info(table_location):


def parse_table(table_name, file_path):
    tb_loc = parse_table_location(file_path).get(table_name)
    start_nrow, end_nrow, sheet_name = tb_loc.get('start_nrow'), tb_loc.get('end_nrow'), tb_loc.get('sheet_name')
    work_sheet = xlrd.open_workbook(file_path, encoding_override='utf_8').sheet_by_name(sheet_name)
    data = _parse_table(table_name, start_nrow, end_nrow, work_sheet)
    return data


@fn_timer
def parse_org_info(org_id, file_path):
    data = parse_table('org_info', file_path)
    data_client = data.iloc[8:10].set_index([['c', 'v']]).T.set_index(['c']).drop('客户结构').T
    data.drop(data.iloc[8:10].index, inplace=True)
    data_left = data.iloc[:, :2].T.set_index([['c', 'v']]).T.set_index(['c']).T
    data_right = data.iloc[:, 2:4].dropna(how='all').T.set_index([['c', 'v']]).T.set_index(['c']).T
    data = pd.concat([data_left, data_client, data_right], axis=1)
    data.loc[:, 'org_id'] = org_id
    return data


def parse_shareholder(org_id, file_path):
    data = parse_table('shareholder', file_path)
    # shareholder_condition: 简要介绍公司主要股东和实际控制人情况
    sh_cond = data.iloc[[-1], :2].T.set_index([['c', 'v']]).T.set_index(['c']).T

    # shareholder_info: 股东信息表
    sh_info = data.iloc[:-1, :]
    sh_info.columns = sh_info.iloc[0, :].tolist()
    sh_info = sh_info.iloc[1:, :]
    sh_info['org_id'] = org_id

    # TODO  1. sh_info 入库
    return sh_cond


def parse_department(org_id, file_path):
    data = parse_table('department', file_path)

    # department_info: 组织结构
    department_info = data.iloc[:-1, :]
    department_info.columns = department_info.iloc[0, :].tolist()
    department_info = department_info.iloc[1:, :]
    department_info['org_id'] = org_id

    # TODO  1. department_info 入库
    # TODO  2. 公司组织结构图
    return True


def parse_operation_state(org_id, file_path):
    data_state = parse_table('operation_state', file_path)
    data_asset = parse_table('asset_scale', file_path)
    data_prize = parse_table('prize', file_path)
    state_info = pd.DataFrame(data_state.iloc[1:, 1:].as_matrix(),
                              index=data_state.iloc[1:, 0].tolist(),
                              columns=data_state.iloc[0, 1:].tolist())
    asset_info = pd.DataFrame(data_asset.iloc[1:, 1:].as_matrix(),
                              index=data_asset.iloc[1:, 0].tolist(),
                              columns=data_asset.iloc[0, 1:].tolist())
    data_prize.fillna('', inplace=True)
    prize_info = pd.DataFrame([[PLACEHOLDER.join(data_prize.iloc[1:, _].tolist()) for _ in range(1, 4)]],
                              index=[data_prize.iloc[0, 0]],
                              columns=data_prize.iloc[0, 1:].tolist())
    operation_state_info = pd.concat([state_info, asset_info, prize_info], axis=1)
    operation_state_info.loc[:, 'org_id'] = org_id
    return operation_state_info


def parse_team_scale(org_id, file_path):
    data = parse_table('team_scale', file_path)

    team_scale_info = data
    team_scale_info.columns = team_scale_info.iloc[0, :].tolist()
    team_scale_info = team_scale_info.iloc[1:, :]
    team_scale_info['org_id'] = org_id
    return team_scale_info


def parse_core_member(org_id, file_path):
    data = parse_table('core_member', file_path)

    core_member_info = data
    core_member_info.columns = core_member_info.iloc[0, :].tolist()
    core_member_info = core_member_info.iloc[1:, :]
    core_member_info['org_id'] = org_id

def _parse_rows(row_cells):
    print(row_cells)
    cells = [_.value if _.ctype != 3 else dt.date(*xlrd.xldate_as_tuple(_.value, datemode=0)[:3]) for _ in row_cells]
    return cells


@fn_timer
def _parse_table_rows(start_nrow, end_nrow, ws):
    table_rows = []
    row_num = start_nrow + 1
    while row_num < end_nrow:
        info_row = _parse_rows(ws.row(row_num))
        table_rows.append(info_row)
        row_num += 1
    return table_rows


@fn_timer
def _table_to_dataframe(table_rows):
    table = pd.DataFrame(table_rows).applymap(lambda x: x if x else np.NaN).dropna(how='all').dropna(how='all', axis=1)
    return table


@fn_timer
def _parse_table(table_name, start_nrow, end_nrow, ws):
    _SIZE = TABLE_SIZE.get(table_name)
    _info = _table_to_dataframe(_parse_table_rows(start_nrow, end_nrow, ws))
    if isinstance(_SIZE[0], int):
        if isinstance(_SIZE[1], int):
            return _info.iloc[:_SIZE[0], :_SIZE[1]]
        else:
            return _info.iloc[:_SIZE[0], :]
    else:
        if isinstance(_SIZE[1], int):
            return _info.iloc[:, :_SIZE[1]]
        else:
            return _info.iloc[:, :]


def sort_ordered_dict(od: OrderedDict, key=None, ascending=True):
    k, v = list(od.keys()), list(od.values())
    temp = dict(zip(v, k))
    v.sort(key=key, reverse=not ascending)
    return OrderedDict([(temp.get(_), _) for _ in v])


if __name__ == '__main__':
    file_path = "C:\\Users\\Zhan\\PycharmProjects\\DueDiligence\\FOF尽调模板1009.xlsx"

    # wb = xlrd.open_workbook(path)
    # ws = wb.sheet_by_index(1)
    # s = parse_org_info('abc', path)
    # print(s)
