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
    now = datetime.datetime.now()
    thirty_days_ago = now - datetime.timedelta(days=360)
    return utc_datetime < thirty_days_ago

def parse_error_content(dns_content):
    error_match = re.match(r"Error: \((\d+), '(.+)'\)", dns_content)
    if error_match:
        error_type = error_match.group(1)
        error_description = error_match.group(2)
        return error_type, error_description
    else:
        return None, dns_content

def removeDirLongerThanMonth(task_path):
    subTasks = [os.path.join(task_path, f) for f in os.listdir(task_path) if os.path.isdir(os.path.join(task_path, f))]
    for subTask in subTasks:
        time = subTask.split('_')[-1]
        if longerThenMonth(int(time)):
            shutil.rmtree(subTask)
    return

class ipInfo():
    def __init__(self, ip, nodeType, dns):
        self.ip = ip
        self.nodeType = nodeType
        self.dns = dns

    def ip_equl(self, ip):
        return ip == self.ip

def is_ip(ip_str):
    pattern = r'^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$'
    return bool(re.match(pattern, ip_str))

def getNodeIp(filename):
    ip_match = re.match(r"^([\d.]+)-", os.path.basename(filename))
    return ip_match.group(1) if ip_match else ""

def readDataFromJsonByDomain(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        node_ip = getNodeIp(filename)
        data = json.load(f)
    return data, node_ip

def readDataFromDomain(domain_data, domain_ip, node_ip):
    for domain, dns in domain_data.items():
        if domain not in domain_ip.keys():
            domain_ip[domain] = set()
        for dns_ip, domain_ips in dns.items():
            for domain_ip in domain_ips:
                if is_ip(domain_ip):
                    domain_ip[domain].add(ipInfo(domain_ip, node_ip, dns_ip))

def getJsonName(dir):
    files_and_folders = os.listdir(dir)
    json_files_insubtask = [os.path.join(dir, f) for f in files_and_folders if f.endswith('.json')]
    return json_files_insubtask

def readDataFromJson(folder_path):
    subTasks = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]
    json_files = []
    for subTask in subTasks:
        json_files_insubtask = getJsonName(subTask)
        json_files.extend(json_files_insubtask)

    domain_ip = {}
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            node_ip = getNodeIp(json_file)
            datas = json.load(f)
            if isinstance(datas, dict):
                readDataFromDomain(datas, domain_ip, node_ip)
            elif isinstance(datas, list):
                data = datas[1]
                readDataFromDomain(data, domain_ip, node_ip)
    return domain_ip

def calculateDNSIpCtrlRate(dnsIpList, polluteIpList):
    dnsLen = len(dnsIpList)
    if dnsLen == 0:
        print("dnsIpList 长度为零，返回率为 0")
        return 0
    dnsIpInPolluteIpList = sum(1 for dnsIp in dnsIpList if dnsIp in polluteIpList)
    rate = dnsIpInPolluteIpList / dnsLen
    print(f"计算的DNS控制率: {rate}")
    return rate

def calculateDNSIpCtrlRateAndRecordJson(polluteIpFileName, taskFilePath):
    polluteIpList = readPolluteIp(polluteIpFileName)
    monitor_results_set = set()
    fileNames = getJsonName(taskFilePath)
    taskCrtlRate = {}
    nodeTypeRate = {}

    for filename in fileNames:
        data, node_ip = readDataFromJsonByDomain(filename)
        if not node_ip:
            print(f"跳过文件 {filename}，未找到节点IP")
            continue

        nodeTypeRate[node_ip] = []
        full_ctrl_data = []
        for domain, dns in data.items():
            if domain not in taskCrtlRate:
                taskCrtlRate[domain] = {}

            for dnsIp, dnsContent in dns.items():
                _filename = os.path.basename(filename)
                for content in dnsContent:
                    if content.startswith("Error:"):
                        error_type, error_description = parse_error_content(content)
                        insertToError(_filename, node_ip, domain, dnsIp, error_type, error_description)
                        print(f"错误: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}, type {error_type}, error {error_description}")
                        continue

                    rate = calculateDNSIpCtrlRate(dnsContent, polluteIpList)
                    nodeTypeRate[node_ip].append(rate)
                    try:
                        time = datetime.datetime.strptime(_filename.split('_')[1], '%Y-%m-%d-%H-%M-%S').strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError as e:
                        print(f"解析时间失败: {_filename} - 错误信息: {e}")
                        continue

                    for ip in dnsContent:
                        ctrl = ip in polluteIpList
                        record_identifier = (_filename, node_ip, domain, dnsIp, ip, ctrl)
                        if record_identifier not in monitor_results_set:
                            monitor_results_set.add(record_identifier)
                            insertToMonitorResults(_filename, node_ip, domain, dnsIp, ip, ctrl)
                            print(f"插入 MonitorResults: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}, ip {ip}, ctrl {ctrl}")

                    if rate == 1:
                        insertToFullCtrl(_filename, node_ip, domain, dnsIp)
                        print(f"插入 FullCtrl: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}")
                        full_ctrl_data = []
                    elif rate == 0:
                        for ip in dnsContent:
                            insertToFullEscape(_filename, node_ip, domain, dnsIp, ip)
                            print(f"插入 FullEscape: task {_filename}, node {node_ip}, domain {domain}, dnsip {dnsIp}, ip {ip}")

                    if dnsIp not in taskCrtlRate[domain]:
                        taskCrtlRate[domain][dnsIp] = []
                    taskCrtlRate[domain][dnsIp].append(node_ip + ":" + str(rate))

        if full_ctrl_data:
            insertToFullCtrl(full_ctrl_data)

    domain_nodejsonpath = os.path.join(taskFilePath, "domain_node.json")
    with open(domain_nodejsonpath, "w", encoding="utf-8") as f:
        json.dump(taskCrtlRate, f, indent=4)
    print(f"完成JSON文件写入: {domain_nodejsonpath}")
    return taskCrtlRate, nodeTypeRate, time

def mean(values):
    return sum(values) / len(values) if values else 0

def calculate(csvName, task_path):
    subTasks = [os.path.join(task_path, f) for f in os.listdir(task_path) if os.path.isdir(os.path.join(task_path, f))]
    subTaskCtrlRates = {}
    subTaskNodeCtrlRates = {}

    for subTask in subTasks:
        taskCrtlRates, nodeTypeRates, time = calculateDNSIpCtrlRateAndRecordJson(csvName, subTask)
        subTaskCtrlRates[time] = taskCrtlRates
        subTaskNodeCtrlRates[time] = nodeTypeRates

        NodeMeanRatesDice = {nodeType: mean(nodeTypeRate) for nodeType, nodeTypeRate in nodeTypeRates.items()}
        for nodeType, rate in NodeMeanRatesDice.items():
            print(f"准备插入 NodeCtrlRate: time={time}, nodeType={nodeType}, rate={rate}")
            insertToNodeCtrlRate(time, nodeType, rate)

        taskMeanRate = mean(list(NodeMeanRatesDice.values()))
        print(f"准备插入 TaskCtrlRate: time={time}, taskMeanRate={taskMeanRate}")
        insertToTaskCtrlRate(time, taskMeanRate)

    dnsCtrlRates = {}
    domainCtrlRates = {}

    for time, task in subTaskCtrlRates.items():
        for domainName, dns in task.items():
            if domainName not in domainCtrlRates:
                domainCtrlRates[domainName] = []
            for dnsIp, nodes in dns.items():
                if dnsIp not in dnsCtrlRates:
                    dnsCtrlRates[dnsIp] = []
                for node in nodes:
                    rate = float(node.split(":")[1])
                    dnsCtrlRates[dnsIp].append(rate)
                    domainCtrlRates[domainName].append(rate)

    for domainName, rates in domainCtrlRates.items():
        mean_rate = mean(rates)
        print(f"准备插入 DomainCtrlRate: time={time}, domain={domainName}, rate={mean_rate}")
        insertToDomainCtrlRate(time, domainName, mean_rate)

    for dnsIp, rates in dnsCtrlRates.items():
        mean_rate = mean(rates)
        print(f"准备插入 DnsCtrlRate: time={time}, dnsIp={dnsIp}, rate={mean_rate}")
        insertToDnsCtrlRate(time, dnsIp, mean_rate)

    return subTaskCtrlRates, subTaskNodeCtrlRates


task_path = "/home/ubuntu/zsa/dns_lsm/app/task/task_1729920543/"
removeDirLongerThanMonth(task_path)
calculateDNSIpCtrlRateAndRecordJson("ips.csv", task_path)
