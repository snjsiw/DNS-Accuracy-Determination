import mysql.connector

cursor = None

def connect_to_db(func):
    def wrapper(*args, **kwargs):
        # Connect to MySQL database
        conn = mysql.connector.connect(
            host='8.210.155.15',
            user='root',
            password='password',
            database='dns'
        )

        # Create a cursor object
        global cursor
        cursor = conn.cursor()

        # Call the original function
        result = func(*args, **kwargs)
        cursor = None

        # Commit changes and close the connection
        conn.commit()
        conn.close()

        # Return the result of the original function
        return result

    return wrapper


@connect_to_db
def insetrToFullEscape(task, node, domain, DNS, IP):
    global cursor
    cursor.execute("INSERT IGNORE INTO full_escape VALUES (%s, %s, %s, %s, %s)", (task, node, domain, DNS, IP))


@connect_to_db
def create_FullEscape():
    global cursor
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS full_escape (
        task VARCHAR(255),
        node VARCHAR(255),
        domain VARCHAR(255),
        dns VARCHAR(255),
        ip VARCHAR(255)
    )
    """)


@connect_to_db
def create_TaskCtrlRate():
    global cursor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS taskCtrlRate (
            time DATETIME,
            ctrlRate FLOAT
        );
    """)





@connect_to_db
def create_TimeOutRate():
    global cursor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS TimeOutRate (
            task VARCHAR(255),
            node VARCHAR(255),
            domain VARCHAR(255),
            dns VARCHAR(255)
        );
    """)


@connect_to_db
def insetrToTimeOutRate(task, node, domain, DNS):
    global cursor
    cursor.execute("INSERT IGNORE INTO TimeOutRate VALUES (%s, %s, %s, %s)", (task, node, domain, DNS))


@connect_to_db
def insetrToTaskCtrlRate(time, rate):
    global cursor
    cursor.execute("INSERT IGNORE INTO taskCtrlRate VALUES (%s, %s)", (time, rate))


@connect_to_db
def createFULL_CONTROL_table():
    global cursor
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS full_control (
        task VARCHAR(255),
        node VARCHAR(255),
        domain VARCHAR(255),
        dns VARCHAR(255)
    )
    """)


# @connect_to_db
# def insertToFullCtrl(task, node, domain, DNS):
#     global cursor
#     cursor.execute("INSERT IGNORE INTO full_control VALUES (%s, %s, %s, %s)", (task, node, domain, DNS))

@connect_to_db
def insertToFullCtrl(batch_data):
    global cursor
    # 构建SQL语句用于批量插入
    query = "INSERT IGNORE INTO full_control VALUES (%s, %s, %s, %s)"
    # 使用executemany进行批量插入
    cursor.executemany(query, batch_data)

@connect_to_db
def checkTable(s):
    global cursor
    cursor.execute(f"SELECT * FROM {s}")
    results = cursor.fetchall()
    for row in results:
        task = row[0]
        node = row[1]
        domain = row[2]
        dns = row[3]
        print(f"任务: {task}, 节点: {node}, 域名: {domain}, DNS: {dns}")


@connect_to_db
def create_NodeCtrlRate():
    global cursor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NodeCtrlRate(
            time DATETIME,
            node VARCHAR(255),
            ctrlRate FLOAT
        );
    """)


@connect_to_db
def insetrToNodeCtrlRate(time, node, rate):
    global cursor
    cursor.execute("INSERT IGNORE INTO NodeCtrlRate VALUES (%s, %s, %s)", (time, node, rate))


@connect_to_db
def create_DnsCtrlRate():
    global cursor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DnsCtrlRate(
            time DATETIME,
            dns VARCHAR(255),
            ctrlRate FLOAT
        );
    """)


@connect_to_db
def insetrToDnsCtrlRate(time, dns, rate):
    global cursor
    cursor.execute("INSERT IGNORE INTO DnsCtrlRate VALUES (%s, %s, %s)", (time, dns, rate))


@connect_to_db
def create_DomainCtrlRate():
    global cursor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DomainCtrlRate(
            time DATETIME,
            domain VARCHAR(255),
            ctrlRate FLOAT
        );
    """)


@connect_to_db
def insetrToDomainCtrlRate(time, domain, rate):
    global cursor
    cursor.execute("INSERT IGNORE INTO DomainCtrlRate VALUES (%s, %s, %s)", (time, domain, rate))


@connect_to_db
def clearTable(table):
    global cursor
    cursor.execute('DELETE FROM ' + table + ';')


@connect_to_db
def showAllTables():
    global cursor
    cursor.execute("SHOW TABLES;")
    # Get the query result
    tables = cursor.fetchall()
    # Output all table names
    for table in tables:
        print(table[0])


def createAllTables():
    create_NodeCtrlRate()
    create_DnsCtrlRate()
    create_DomainCtrlRate()
    create_FullEscape()
    createFULL_CONTROL_table()
    create_TaskCtrlRate()
    create_TimeOutRate()


createAllTables()
showAllTables()