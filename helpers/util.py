import cx_Oracle

def connect(type_of_user: str):
    
    if type_of_user.lower() == 'test_dev_2':
        username = 'TEST_DEV_2'
        password = '###'
        host = '###'
        port = '###'
        service_name = '###'
    elif type_of_user.lower() == 'vertica':
        username = 'VERTICA_STAT'
        password = '###'
        host = '###'
        port = '###'
        service_name = '###'   
    else:
        username = 'jump'
        password = '###'
        host = '###'
        port = '###'
        service_name = '###'
    if username and password and host and port and service_name:
        connect_str = f'(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))(CONNECT_DATA = (SERVICE_NAME = {service_name})))'
        con = cx_Oracle.connect(username, password, connect_str, encoding='utf-8')
        cur = con.cursor()
        return con, cur
    else:
        raise Exception("Type of connection not recognised.")