#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import argparse
import logging
from pygresql.pg import DB
from multiprocessing import Process


"""The golden rule
https://groups.google.com/a/greenplum.org/g/gpdb-dev/c/rSacd_vI-fM/m/pkAW-Z-lCgAJ
If a partitioned table is Hash distributed, then all its leaf partitions
must also be Hash partitioned on the same distribution key, with the
same 'numsegments', or randomly distributed.
If a partitioned table is Randomly distributed, then all the leafs must
be leaf partitioned as well.
"""

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

# relname = foo.bar
def get_child_names_of_root(relname, dbname, port, host, user):
    db = DB(dbname=dbname, host=host, port=port, user=user)
    schema_name, table_name = relname.split('.')
    
    sql = ("select partitionschemaname || '.' || partitiontablename from pg_partitions "
           "where tablename = '{table_name}' and partitionschemaname='{schema_name}'").format(table_name=table_name, schema_name=schema_name)
    r = db.query(sql).getresult()
    db.close()
    return [p[0] for p in r]

# names should be fully qualified
def get_oid_list(names):
    return ", ".join(["'%s'::regclass::oid" % name
                      for name in names])

# we do not need to handle random policy table
## relname (str): root partition's name fully qualified
## childs  ([str]): all the leafs' name fully qualified
## new_cluster_size (int): the cluster size after expansion
def step1(relname, childs, dbname, port, host, new_cluster_size, user):
    """
    step1:
      in a single transaction change root+all leafs's
      policy.numsegments = full cluster size;
      change all leafs to randomly dist.
    after step1:
      root is hash on all segs (full cluster size)
      leafs is random on all segs
    !!!!!! NOTE
    We simply update the gp_policy catalog here, it should
    be OK and do no harm except these statements are not
    dispatched to QEs, so gp_policy will not be consistent.
    We can fix this later or we can write UDFs here.
    """
    all_parts_with_root_names = [relname] + childs
    db = DB(dbname=dbname, host=host, port=port, user=user)
    db.query("set allow_system_table_mods = on;")
    db.query("begin;")

    ## lock the root and all leafs
    my_print("Step 1: Trying to grab ACCESS EXCLUSIVE lock on root and all childs: root is {relname}".format(relname=relname))
    db.query("lock {relname} IN ACCESS EXCLUSIVE MODE".format(relname=relname))

    ## Check if step1 is needed
    sql0 = ("select numsegments from gp_distribution_policy "
            "where localoid in ({oid_list})").format(oid_list=get_oid_list(all_parts_with_root_names))
    r = db.query(sql0).getresult()
    numsegments = [tp[0] for tp in r if tp[0] == new_cluster_size]
    ## either we are done step1 or we do not touch any of root and leafs
    assert(len(numsegments) == 0 or #never touch
           len(numsegments) == len(all_parts_with_root_names)) #done
    if len(numsegments) == len(all_parts_with_root_names):
        my_print("Step1: all is done, skip step 1 for this run")
        db.query("end;")
        db.close()
        return

    sql1 = ("update gp_distribution_policy "
            "set numsegments = {new_cluster_size} "
            "where localoid in ({oid_list})").format(new_cluster_size=new_cluster_size,
                                                     oid_list=get_oid_list(all_parts_with_root_names))
    db.query(sql1)
    sql2 = ("update gp_distribution_policy "
            "set distkey = '', distclass = '' "
            "where localoid in ({oid_list})").format(oid_list=get_oid_list(childs))
    db.query(sql2)
    db.query("end;")
    db.close()
    my_print("Step 1 complete: Distribution policies of root and leaf partitions updated for {relname}".format(relname=relname))

# child is fully qualified
def step2_one_rel(child, db, distkey, distclass, distby):
    db.query("begin;")
    my_print("Step 2: Trying to grab ACCESS EXCLUSIVE lock on child: {relname}".format(relname=child))
    db.query("lock {relname} IN ACCESS EXCLUSIVE MODE".format(relname=child))
    my_print("Step 2: Successfully grabbed ACCESS EXCLUSIVE lock on child: {relname}".format(relname=child))

    ## Santiy Check if the rel is already hash dist
    ## If so, we just skip. This makes the script
    ## can be killed and then re-continue.
    sql0 = ("select distkey from gp_distribution_policy "
            "where localoid = '{relname}'::regclass::oid").format(relname=child)
    r = db.query(sql0).getresult()[0][0]
    if r != '':
        my_print("Step 2: the leaf {child} is done, skip it".format(child=child))
        db.query("end;")
        return

    sql1 = ("update gp_distribution_policy "
            "set distkey = '{distkey}', distclass = '{distclass}' "
            "where localoid = '{relname}'::regclass::oid").format(distkey=distkey,
                                                                  distclass=distclass,
                                                                  relname=child)
    db.query(sql1)

    sql2 = ("alter table {relname} set with (REORGANIZE=true) "
            "distributed by ({distby})").format(distby=distby, relname=child)
    my_print("Step 2: beginning alter table REORGANIZE on {relname}:".format(relname=child))

    db.query(sql2)
    db.query("end;")
    my_print("Step 2: finished alter table REORGANIZE on {relname}:".format(relname=child))

def step2_worker(wid, concurrency, childs, dbname, port, host, distkey, distclass, distby, user):
    db = DB(dbname=dbname, host=host, port=port, user=user)
    db.query("set allow_system_table_mods = on;")
    for id, child in enumerate(childs):
        if id % concurrency == wid:
            step2_one_rel(child, db, distkey, distclass, distby)
    db.close()

# relname should be fully qualified
def get_dist_info(relname, dbname, port, host, user):
    db = DB(dbname=dbname, host=host, port=port, user=user)
    sql = ("select distkey, distclass from gp_distribution_policy "
           "where localoid = '{relname}'::regclass::oid").format(relname=relname)
    my_print("get_dist_info prints %s" %sql)
    r = db.query(sql).getresult()
    db.close()
    return r[0]

## relname (str): root partition's name 
## childs  ([str]): all the leafs' name
## concurrency(int): how many leafs to expand at the same time
## distby  (str): root partition's distby, like "c1,c2"
def step2(relname, childs, dbname, port, host, concurrency, distby, user):
    distkey, distclass = get_dist_info(relname, dbname, port, host, user)
    ps = []
    for i in range(concurrency):
        p = Process(target=step2_worker, args=(i, concurrency, childs,
                                               dbname, port, host,
                                               distkey, distclass, distby, user))
        p.start()
        ps.append(p)
    for p in ps:
        p.join()

   
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Expand leafs one by one')
    parser.add_argument('--root', type=str, help='root partition name (fully qualified)', required=True)
    parser.add_argument('--njobs', type=int, help='number of concurrent leafs to expand at the same time', required=True)
    parser.add_argument('--newsize', type=int, help='cluster size after expansion', required=True)
    parser.add_argument('--distby', type=str, help='root table distby clause, like "c1, c2"', required=True)
    parser.add_argument('--dbname', type=str, help='database name to connect', required=True)
    parser.add_argument('--host', type=str, help='hostname to connect', required=True)
    parser.add_argument('--childrenfile', type=str, help='file containing fully qualified child partition names') # each line will contain a single name to be done in order
    parser.add_argument('--port', type=int, help='port to connect', required=True)
    parser.add_argument('--user', type=str, help='username to connect with', required=True)
    parser.add_argument('--log', type=str, help='log file path', required=True)

    args = parser.parse_args()
    
    root = args.root
    dbname = args.dbname
    port = args.port
    host = args.host
    njobs = args.njobs
    newsize = args.newsize
    distby = args.distby
    childrenfile = args.childrenfile
    user = args.user
    LOG_PATH = args.log

    # Populate fqns of children from childrenfile OR derive from root partition name
    if not childrenfile:
        childs = get_child_names_of_root(root, dbname, port, host, user)
    else:
        with open(childrenfile) as fp:
            childs = [line.strip() for line in fp]

    all_childs = get_child_names_of_root(root, dbname, port, host, user)
    
    step1(root, all_childs, dbname, port, host, newsize, user)
    step2(root, childs, dbname, port, host, njobs, distby, user)
