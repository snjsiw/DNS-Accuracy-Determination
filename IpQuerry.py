import os
import json
import csv
import re
from sqlManage import *
import datetime
import shutil
import concurrent.futures
def longerThenMonth(timestamp):
    utc_datetime = datetime.datetime.utcfromtimestamp(timestamp)
    # 获取当前日期和时间
    now = datetime.datetime.now()
    # 推算30天前的日期和时间
    thirty_days_ago = now - datetime.timedelta(days=360)
    # 判断目标日期是否早于30天前的日期
    if utc_datetime < thirty_days_ago:
        return True
    else:
        return False


def parse_error_content(dns_content):
    # 使用正则表达式解析错误类型和描述
    error_match = re.match(r"Error: \((\d+), '(.+)'\)", dns_content)
    if error_match:
        error_type = error_match.group(1)
        error_description = error_match.group(2)
        return error_type, error_description
    else:
        return None, dns_content

def removeDirLongerThanMonth(task_path):
    subTasks = [task_path+'//'+f for f in os.listdir(task_path) if os.path.isdir(os.path.join(task_path, f))]
    for subTask in subTasks:
        time=subTask.split('_')[-1]
        if  longerThenMonth(int(time)):
            shutil.rmtree(subTask)
    return
class ipInfo():
    def __init__(self,ip,nodeType,dns) -> None:
        self.ip = ip
        self.nodeType = nodeType
        self.dns = dns

    def ip_equl(self,ip):
        return ip==self.ip


def is_ip(ip_str):
    # 正则表达式匹配IP地址
    pattern = r'^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$'
    return bool(re.match(pattern, ip_str))

def getNodeIp(filename):
    # 提取文件名中的IP地址部分，假设IP格式始终出现在文件名前缀的第一个部分
    ip_match = re.match(r"^([\d.]+)-", os.path.basename(filename))
    if ip_match:
        return ip_match.group(1)
    else:
        return ""

def readDataFromJsonByDomain(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        node_ip = getNodeIp(filename)  # 从文件名提取节点IP
        data = json.load(f)
    return data, node_ip  # 返回数据和node_ip

def readDataFromDomain(domain_data, domain_ip, node_ip):
    # 更新函数参数为 node_ip
    for domain, dns in domain_data.items():
        if domain not in domain_ip.keys():
            domain_ip[domain] = set()
        for dns_ip, domain_ips in dns.items():
            for domain_ip in domain_ips:
                if is_ip(domain_ip):
                    domain_ip[domain].add(ipInfo(domain_ip, node_ip, dns_ip))  # 使用 node_ip 代替 nodetype

def getJsonName(dir):
    files_and_folders = os.listdir(dir)
    # 获取所有.json结尾的文件名
    json_files_insubtask = [dir + '//' + f for f in files_and_folders if f.endswith('.json')]
    return json_files_insubtask

def readDataFromJson(folder_path):
    subTasks = [folder_path + '//' + f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]

    json_files = []
    for subTask in subTasks:
        json_files_insubtask = getJsonName(subTask)
        json_files.extend(json_files_insubtask)

    domain_ip = {}
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            node_ip = getNodeIp(json_file)  # 提取节点IP
            datas = json.load(f)
            if isinstance(datas, dict):
                readDataFromDomain(datas, domain_ip, node_ip)
            elif isinstance(datas, list):
                data = datas[1]
                readDataFromDomain(data, domain_ip, node_ip)
    return domain_ip

def calculateDNSIpCtrlRate(dnsIpList,polluteIpList):
    dnsLen=len(dnsIpList)
    dnsIpInPolluteIpList=0
    for dnsIp in dnsIpList:
        if dnsIp in polluteIpList:
            dnsIpInPolluteIpList+=1
    return dnsIpInPolluteIpList/dnsLen


def calculateDNSIpCtrlRateAndRecordJson(polluteIpFileName, taskFilePath):
    polluteIpList = readPolluteIp(polluteIpFileName)
    fileNames = getJsonName(taskFilePath)
    taskCrtlRate = {}
    nodeTypeRate = {}

    for filename in fileNames:
        data, node_ip = readDataFromJsonByDomain(filename)
        if node_ip == '':
            continue

        nodeTypeRate[node_ip] = []
        full_ctrl_data = []
        for domain, dns in data.items():
            if domain not in taskCrtlRate.keys():
                taskCrtlRate[domain] = {}

            for dnsIp, dnsContent in dns.items():
                _filename = filename.split('//')[-1]
                for content in dnsContent:
                    # 检查内容是否为错误
                    if content.startswith("Error:"):
                        error_type, error_description = parse_error_content(content)
                        # print(
                        #     f"写入error: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}, type {error_type}, description {error_description}")
                        insetrToError(_filename, node_ip, domain, dnsIp, error_type, error_description)
                        continue

                    rate = calculateDNSIpCtrlRate(dnsContent, polluteIpList)
                    nodeTypeRate[node_ip].append(rate)
                    time = datetime.datetime.strptime(_filename.split('_')[1], '%Y-%m-%d-%H-%M-%S').strftime(
                        '%Y-%m-%d %H:%M:%S')

                    for ip in dnsContent:
                        ctrl = ip in polluteIpList
                        print(
                            f"task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}, ip {ip}, ctrl {ctrl}")
                        insertToMonitorResults(_filename, node_ip, domain, dnsIp, ip, ctrl)

                    # 完全管控和完全逃逸的情况下写入数据库
                    if rate == 1:
                        # print(f"写入fullCtrl: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}")
                        insertToFullCtrl(_filename, node_ip, domain, dnsIp)
                        full_ctrl_data = []  # 重置列表
                    elif rate == 0:
                        for ip in dnsContent:
                            # print(
                            #     f"写入FullEscape: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}, ip {ip}")
                            insetrToFullEscape(_filename, node_ip, domain, dnsIp, ip)

                    if dnsIp not in taskCrtlRate[domain].keys():
                        taskCrtlRate[domain][dnsIp] = []
                    taskCrtlRate[domain][dnsIp].append(node_ip + ":" + str(rate))

        # 插入 remaining 数据
        if full_ctrl_data:
            # print(f"插入fullCtrl数据: {full_ctrl_data}")
            insertToFullCtrl(full_ctrl_data)

    domain_nodejsonpath = os.path.join(taskFilePath, "domain_node.json")
    with open(domain_nodejsonpath, "w", encoding="utf-8") as f:
        json.dump(taskCrtlRate, f, indent=4)

    return taskCrtlRate, nodeTypeRate, time


def process_subtask(csvName, subTask):
    taskCrtlRates, nodeTypeRates, time = calculateDNSIpCtrlRateAndRecordJson(csvName, subTask)
    return taskCrtlRates, nodeTypeRates, time

def querryDomainFromGlobal(ips_q,domain_ip):
    ip2domains={}
    for ip_q in ips_q:
        for domain,ips in domain_ip.items():
            for ip in ips:
                if ip.ip_equl(ip_q):
                    ip2domains[ip_q]=domain
    return ip2domains

def querryDomainWithDns(ips_q,domain_ip):
    ip2domains={}
    for ip_q in ips_q:
        for domain,ips in domain_ip.items():
            for ip in ips:
                if ip.ip_equl(ip_q):
                    if ip_q not in ip2domains.keys():
                        ip2domains[ip_q]={}
                        ip2domains[ip_q][ip.dns]=domain
                    ip2domains[ip_q][ip.dns]=domain
    return ip2domains

def querryDomainWithNodeType(ips_q,domain_ip):
    ip2domains={}
    for ip_q in ips_q:
        for domain,ips in domain_ip.items():
            for ip in ips:
                if ip.ip_equl(ip_q):
                    if ip_q not in ip2domains.keys():
                        ip2domains[ip_q]={}
                        ip2domains[ip_q][ip.nodeType]=domain
                    ip2domains[ip_q][ip.nodeType]=domain
    return ip2domains

def readPolluteIp(csvName):
    polluteIps=[]
    with open(csvName,'r') as f:
        reader = csv.reader(f)
        for row in reader:
            polluteIps.append(row[0])
    return polluteIps


def mean(list):
    if len(list)==0:
        return 0
    return sum(list)/len(list)


def calculate(csvName, task_path):
    subTasks = [task_path + '//' + f for f in os.listdir(task_path) if os.path.isdir(os.path.join(task_path, f))]
    subTaskCtrlRates = {}
    subTaskNodeCtrlRates = {}

    for subTask in subTasks:
        taskCrtlRates, nodeTypeRates, time = calculateDNSIpCtrlRateAndRecordJson(csvName, subTask)
        subTaskCtrlRates[time] = taskCrtlRates
        subTaskNodeCtrlRates[time] = nodeTypeRates

        NodeMeanRatesDice = {}
        NodeMeanRatesList = []

        for nodeType, nodeTypeRate in nodeTypeRates.items():
            NodeMeanRatesDice[nodeType] = mean(nodeTypeRate)
            NodeMeanRatesList.append(mean(nodeTypeRate))
            # print(f"写入NodeCtrlRate: time {time}, nodeType {nodeType}, rate {NodeMeanRatesDice[nodeType]}")
            insetrToNodeCtrlRate(time, nodeType, NodeMeanRatesDice[nodeType])

        taskMeanRate = mean(NodeMeanRatesList)
        # print(f"写入TaskCtrlRate: time {time}, rate {taskMeanRate}")
        insetrToTaskCtrlRate(time, taskMeanRate)

    # 计算域管控率
    dnsCtrlRates = {}
    domainCtrlRates = {}

    for time, task in subTaskCtrlRates.items():
        for domainName, dns in task.items():
            if domainName not in domainCtrlRates.keys():
                domainCtrlRates[domainName] = []
            for dns, nodes in dns.items():
                if dns not in dnsCtrlRates.keys():
                    dnsCtrlRates[dns] = []
                for node in nodes:
                    rate = float(node[4:])
                    dnsCtrlRates[dns].append(rate)
                    domainCtrlRates[domainName].append(rate)

    for domainName, domainRate in domainCtrlRates.items():
        # print(f"写入DomainCtrlRate: time {time}, domain {domainName}, rate {mean(domainRate)}")
        insetrToDomainCtrlRate(time, domainName, mean(domainRate))

    for dns, dnsRate in dnsCtrlRates.items():
        # print(f"写入DnsCtrlRate: time {time}, dns {dns}, rate {mean(dnsRate)}")
        insetrToDnsCtrlRate(time, dns, mean(dnsRate))

    return subTaskCtrlRates, subTaskNodeCtrlRates

task_path = "/home/ubuntu/zsa/dns_lsm/app/task/task_1729920543"
removeDirLongerThanMonth(task_path)
calculateDNSIpCtrlRateAndRecordJson("ips.csv", task_path)

