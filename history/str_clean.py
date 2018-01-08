# -*- coding: utf8 -*-
# import sys
#
# reload(sys)
# sys.setdefaultencoding('utf-8')

import re
import chardet


def clean_str_strong(str, mode="all"):
    """
    :param str: utf8
    :return:
    """
    if str is None or str == '':
        return str
    if mode == "all":
        str = re.sub('(\s|\\\\r\\\\n)', '', str)
        str = str.replace(u'\u3000', '').replace(u'\xa0', '')
        str = str.replace('"','')
        return str
    elif mode=="edge":
        for sympol in [u'\u3000', u'\xa0', '\r', '\n', '\v', '\t', '\f', ' ','"']:
            str = ' '.join(filter(lambda x: clean_str_strong(x, "all"), str.split(sympol)))
        return str


def strip_title(str, ls=['先生', '女士', '经理', '博士', '硕士', '总裁', '学者', '学士', '董事长']):
    """
    :param str: unicode: 刘洋博士，男，xx成员，……
    :param ls:
    :return:   刘洋
    """
    try:
        if chardet.detect(str)['encoding'] == 'utf-8':
            str = str.decode('utf8')
        else:
            str = str.decode('gbk')
    except ValueError:
        pass

    name = ''
    num = 0
    for n in str:
        if num > 5 or n in [',', '，', ' ', '　', ':', '：']:
            break
        name += n
        num += 1
    if len(name) < 4:
        return name
    for title in ls:
        if title in name:  # 如果有人叫诸葛博士，呵呵
            name = name[0:name.index(title)]
            return name
    # (刘大力曾在招)商银行……
    return name[0:3]


def str_to_cookies(str, cookies={}):
    """

    :param cookies: str
    :return: dict
    """
    cookies = cookies
    strl = str.split('; ')
    for i in strl:
        if i.strip():
            equal_index = i.index('=')
            key = i[0:equal_index]
            value = i[equal_index + 1::]
            cookies[key] = value
    return cookies

#http://paiming.265o.com/  199个姓氏
surnames = [u'\u674e', u'\u738b', u'\u5f20', u'\u5218', u'\u9648', u'\u6768', u'\u8d75', u'\u9ec4', u'\u5468', u'\u5434',
            u'\u5f90', u'\u5b59', u'\u80e1', u'\u6731', u'\u9ad8', u'\u6797', u'\u4f55', u'\u90ed', u'\u9a6c', u'\u7f57',
            u'\u6881', u'\u5b8b', u'\u90d1', u'\u8c22', u'\u97e9', u'\u5510', u'\u51af', u'\u4e8e', u'\u8463', u'\u8427',
            u'\u7a0b', u'\u66f9', u'\u8881', u'\u9093', u'\u8bb8', u'\u5085', u'\u6c88', u'\u66fe', u'\u5f6d', u'\u5415',
            u'\u82cf', u'\u5362', u'\u848b', u'\u8521', u'\u8d3e', u'\u4e01', u'\u9b4f', u'\u859b', u'\u53f6', u'\u960e',
            u'\u4f59', u'\u6f58', u'\u675c', u'\u6234', u'\u590f', u'\u949f', u'\u6c6a', u'\u7530', u'\u4efb', u'\u59dc',
            u'\u8303', u'\u65b9', u'\u77f3', u'\u59da', u'\u8c2d', u'\u5ed6', u'\u90b9', u'\u718a', u'\u91d1', u'\u9646',
            u'\u90dd', u'\u5b54', u'\u767d', u'\u5d14', u'\u5eb7', u'\u6bdb', u'\u90b1', u'\u79e6', u'\u6c5f', u'\u53f2',
            u'\u987e', u'\u4faf', u'\u90b5', u'\u5b5f', u'\u9f99', u'\u4e07', u'\u6bb5', u'\u6f15', u'\u94b1', u'\u6c64',
            u'\u5c39', u'\u9ece', u'\u6613', u'\u5e38', u'\u6b66', u'\u4e54', u'\u8d3a', u'\u8d56', u'\u9f9a', u'\u6587',
            u'\u5e9e', u'\u6a0a', u'\u5170', u'\u6bb7', u'\u65bd', u'\u9676', u'\u6d2a', u'\u7fdf', u'\u5b89', u'\u989c',
            u'\u502a', u'\u4e25', u'\u725b', u'\u6e29', u'\u82a6', u'\u5b63', u'\u4fde', u'\u7ae0', u'\u9c81', u'\u845b',
            u'\u4f0d', u'\u97e6', u'\u7533', u'\u5c24', u'\u6bd5', u'\u8042', u'\u4e1b', u'\u7126', u'\u5411', u'\u67f3',
            u'\u90a2', u'\u8def', u'\u5cb3', u'\u9f50', u'\u6cbf', u'\u6885', u'\u83ab', u'\u5e84', u'\u8f9b', u'\u7ba1',
            u'\u795d', u'\u5de6', u'\u6d82', u'\u8c37', u'\u7941', u'\u65f6', u'\u8212', u'\u803f', u'\u725f', u'\u535c',
            u'\u8def', u'\u8a79', u'\u5173', u'\u82d7', u'\u51cc', u'\u8d39', u'\u7eaa', u'\u9773', u'\u76db', u'\u7ae5',
            u'\u6b27', u'\u7504', u'\u9879', u'\u66f2', u'\u6210', u'\u6e38', u'\u9633', u'\u88f4', u'\u5e2d', u'\u536b',
            u'\u67e5', u'\u5c48', u'\u9c8d', u'\u4f4d', u'\u8983', u'\u970d', u'\u7fc1', u'\u968b', u'\u690d', u'\u7518',
            u'\u666f', u'\u8584', u'\u5355', u'\u5305', u'\u53f8', u'\u67cf', u'\u5b81', u'\u67ef', u'\u962e', u'\u6842',
            u'\u95f5', u'\u6b27\u9633', u'\u89e3', u'\u5f3a', u'\u67f4', u'\u534e', u'\u8f66', u'\u5189', u'\u623f']


if __name__== '__main__' :
    pass