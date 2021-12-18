#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import argparse
import logging
import pygresql.pg as pg
from pygresql.pg import DB

LOG_PATH = ""

def my_print(msg):
    assert(len(LOG_PATH) > 0)
    logger = logging.getLogger(str(os.getpid()))
    if not len(logger.handlers):
        hdlr = logging.FileHandler(LOG_PATH)
        formatter = logging.Formatter('%(asctime)s p-%(process)d %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)
    logger.debug(msg)

def get_all_seg_info(dbname, host, port, user):
    db = DB(dbname=dbname, host=host, port=port, user=user)
    sql = ("select content, hostname, port from gp_segment_configuration "
           "where role = 'p' and content <> -1;")
    r = db.query(sql).getresult()
    db.close()
    return r

def fix_policy(content_id, dbname, seg_host, seg_port, newsize, root_oid, user):
    my_print("start fixing seg<%s>'s policy for table with oid <%s>" % (content_id, root_oid))
    con = pg.connect(dbname=dbname, host=seg_host, port=seg_port,
                     user=user,opt="-c gp_session_role=utility")
    set_guc_sql = "set allow_system_table_mods = on;"
    con.query(set_guc_sql)
    fix_policy_sql = "update gp_distribution_policy set numsegments = {newsize} where localoid = {root_oid};"
    fix_policy_sql = fix_policy_sql.format(newsize=newsize, root_oid=root_oid)
    con.query(fix_policy_sql)
    con.close()
    my_print("fixed seg<%s>'s policy for table with oid <%s>" % (content_id, root_oid))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Expand leafs one by one')
    parser.add_argument('--root_oid', type=int, help='root partition oid', required=True)
    parser.add_argument('--newsize', type=int, help='cluster size after expansion', required=True)
    parser.add_argument('--dbname', type=str, help='database name to connect', required=True)
    parser.add_argument('--host', type=str, help='hostname to connect', required=True)
    parser.add_argument('--port', type=int, help='port to connect', required=True)
    parser.add_argument('--user', type=str, help='username to connect with', required=True)
    parser.add_argument('--log', type=str, help='log file path', required=True)

    args = parser.parse_args()
    
    root_oid = args.root_oid
    dbname = args.dbname
    port = args.port
    host = args.host
    newsize = args.newsize
    user = args.user
    LOG_PATH = args.log

    all_seg_info = get_all_seg_info(dbname, host, port, user)
    for content_id, seg_host, seg_port in all_seg_info:
        fix_policy(content_id, dbname, seg_host, seg_port, newsize, root_oid, user)
