"""
AUTHOR: zhangbz
PROJECT: UrbanPlayground
DATE: 2026/4/13
TIME: 13:55
DESCRIPTION: Tool module that provides functions for command execution and database operations.
"""
import subprocess
import psycopg2


def run_cmd(cmd, print_output=True):
    """Execute command line commands, print output, raise exception on failure."""
    result = subprocess.run(
        cmd, shell=True,
        capture_output=True, text=True
    )
    if result.stdout:
        if print_output:
            print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed, return code: {result.returncode}")
    return result


def get_conn(dbname="Test20260413", user="postgres", password="we6666", host="localhost", port="5432"):
    """Get PostgreSQL database connection."""
    return psycopg2.connect(
        dbname=dbname, user=user,
        password=password, host=host, port=port
    )


def run_sql(sql, fetch=False, conn=None):
    """Execute SQL statements, optionally fetch results."""
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
