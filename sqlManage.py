import pymysql

# 数据库配置
DB_USER_HK = "root"
DB_PASSWORD_HK = "password"
DB_HOST_HK = "150.109.100.62"
DB_PORT_HK = 3306
DB_NAME_HK = "dns"

# 全局 cursor
cursor = None

def connect_to_db(func):
    """装饰器函数用于数据库连接管理"""
    def wrapper(*args, **kwargs):
        # 使用 pymysql 连接数据库
        conn = pymysql.connect(
            host=DB_HOST_HK,
            user=DB_USER_HK,
            password=DB_PASSWORD_HK,
            database=DB_NAME_HK,
            port=DB_PORT_HK
        )
        global cursor
        cursor = conn.cursor()
        result = func(*args, **kwargs)
        conn.commit()
        cursor.close()
        conn.close()
        return result
    return wrapper

@connect_to_db
def create_error():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reError (
            task VARCHAR(100),
            node VARCHAR(100),
            domain VARCHAR(100),
            dns VARCHAR(100),
            type VARCHAR(100),
            description TEXT
        );
    """)

@connect_to_db
def insertToError(task, node, domain, dns, error_type, description):
    cursor.execute("INSERT IGNORE INTO reError VALUES (%s, %s, %s, %s, %s, %s)", (task, node, domain, dns, error_type, description))

@connect_to_db
def create_MonitorResults():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MonitorResults (
            task VARCHAR(100),
            nodeip VARCHAR(100),
            domain VARCHAR(100),
            dns VARCHAR(100),
            ip VARCHAR(45),
            ctrl BOOLEAN,
            UNIQUE(task, nodeip, domain, dns, ip)
        );
    """)

@connect_to_db
def insertToMonitorResults(task, nodeip, domain, dns, ip, ctrl):
    cursor.execute("INSERT IGNORE INTO MonitorResults VALUES (%s, %s, %s, %s, %s, %s)", (task, nodeip, domain, dns, ip, ctrl))

@connect_to_db
def insertToFullEscape(task, node, domain, DNS, IP):
    cursor.execute("INSERT IGNORE INTO full_escape VALUES (%s, %s, %s, %s, %s)", (task, node, domain, DNS, IP))

@connect_to_db
def create_FullEscape():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS full_escape (
        task VARCHAR(100),
        node VARCHAR(100),
        domain VARCHAR(100),
        dns VARCHAR(100),
        ip VARCHAR(100),
        UNIQUE(task, node, domain, dns, ip)
    );
    """)

@connect_to_db
def create_TaskCtrlRate():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS taskCtrlRate (
            time DATETIME,
            ctrlRate FLOAT,
            UNIQUE(time)
        );
    """)

@connect_to_db
def insertToTaskCtrlRate(time, rate):
    cursor.execute("INSERT IGNORE INTO taskCtrlRate VALUES (%s, %s)", (time, rate))

@connect_to_db
def createFULL_CONTROL_table():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS full_control (
        task VARCHAR(100),
        node VARCHAR(100),
        domain VARCHAR(100),
        dns VARCHAR(100),
        UNIQUE(task, node, domain, dns)
    );
    """)

@connect_to_db
def insertToFullCtrl(task, node, domain, DNS):
    cursor.execute("INSERT IGNORE INTO full_control VALUES (%s, %s, %s, %s)", (task, node, domain, DNS))

@connect_to_db
def create_NodeCtrlRate():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NodeCtrlRate(
            time DATETIME,
            node VARCHAR(100),
            ctrlRate FLOAT,
            UNIQUE(time, node)
        );
    """)

@connect_to_db
def insertToNodeCtrlRate(time, node, rate):
    cursor.execute("INSERT IGNORE INTO NodeCtrlRate VALUES (%s, %s, %s)", (time, node, rate))

@connect_to_db
def create_DnsCtrlRate():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DnsCtrlRate(
            time DATETIME,
            dns VARCHAR(100),
            ctrlRate FLOAT,
            UNIQUE(time, dns)
        );
    """)

@connect_to_db
def insertToDnsCtrlRate(time, dns, rate):
    cursor.execute("INSERT IGNORE INTO DnsCtrlRate VALUES (%s, %s, %s)", (time, dns, rate))

@connect_to_db
def create_DomainCtrlRate():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DomainCtrlRate(
            time DATETIME,
            domain VARCHAR(100),
            ctrlRate FLOAT,
            UNIQUE(time, domain)
        );
    """)

@connect_to_db
def insertToDomainCtrlRate(time, domain, rate):
    cursor.execute("INSERT IGNORE INTO DomainCtrlRate VALUES (%s, %s, %s)", (time, domain, rate))

@connect_to_db
def clearTable(table):
    cursor.execute(f"DELETE FROM {table};")

@connect_to_db
def showField(table):
    cursor.execute(f"DESCRIBE {table}")
    result = cursor.fetchall()
    for column in result:
        print(column[0])

@connect_to_db
def showAllTables():
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    for table in tables:
        print(table[0])

def createAllTables():
    create_NodeCtrlRate()
    create_DnsCtrlRate()
    create_DomainCtrlRate()
    create_FullEscape()
    createFULL_CONTROL_table()
    create_TaskCtrlRate()
    create_error()
    create_MonitorResults()

createAllTables()
showAllTables()
