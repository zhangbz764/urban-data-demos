"""
AUTHOR: zhangbz
PROJECT: UrbanPlayground
DATE: 2026/4/13
TIME: 13:55
DESCRIPTION: 工具模块，提供命令执行和数据库操作函数。
"""
import subprocess
import psycopg2


def run_cmd(cmd):
    """执行命令行命令，打印输出，失败时抛异常。"""
    result = subprocess.run(
        cmd, shell=True,
        capture_output=True, text=True
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"命令失败，返回码：{result.returncode}")
    return result


def get_conn(dbname="Test20260413", user="postgres", password="we6666", host="localhost", port="5432"):
    """获取PostgreSQL数据库连接。"""
    return psycopg2.connect(
        dbname=dbname, user=user,
        password=password, host=host, port=port
    )


def run_sql(sql, fetch=False, conn=None):
    """执行SQL语句，可选获取结果。"""
    created_conn = False
    if conn is None:
        conn = get_conn()
        created_conn = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        if fetch:
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if created_conn:
            conn.close()
