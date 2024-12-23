# encoding:utf-8
"""
清理超过一定时长的域名的dns数据
"""
import os
import datetime
import shutil


def data_clean_folders(root_folder, days=1):
    """
    """
    folders = os.listdir(root_folder)
    for t in folders:
        folder_address = os.path.join(root_folder, t)
        dir_time = datetime.datetime.fromtimestamp(os.path.getmtime(folder_address))
        now = datetime.datetime.now()
        del_time_limit = datetime.timedelta(hours=24*days)
        # del_time_limit = datetime.timedelta(minutes=10)   # 测试
        if del_time_limit < (now-dir_time):
            shutil.rmtree(folder_address)


def data_clean_files(root_folder, days=1):
    """
    删除当前目录下的domain_dns_date文件夹里的数据文件,保留最近10天的数据
    """

    file_name = os.listdir(root_folder)
    for t in file_name:
        file_address = root_folder+'/'+t
        dir_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_address))
        now = datetime.datetime.now()
        del_time_limit = datetime.timedelta(hours=24*days)
        # del_time_limit = datetime.timedelta(minutes=10)
        if del_time_limit < (now-dir_time):
            os.remove(file_address)