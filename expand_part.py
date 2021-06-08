#!/usr/bin/env python
#-*- coding: utf-8 -*-

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

def get_child_of_root(relname):
    c = []
    with open("/home/gpadmin/test/childs") as f:
        for line in f:
            c.append(line.strip().strip(","))
    return c

def get_oid_list(names):
    return ", ".join(["'%s'::regclass::oid" % name
                      for name in names])

# we do not need to handle random policy table
def step1(relname, childs, dbname, port, host, new_cluster_size):
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
    all_parts_with_root = [relname] + childs
    db = DB(dbname=dbname, host=host, port=port)
    db.query("set allow_system_table_mods = on;")
    db.query("begin;")
    sql1 = ("update gp_distribution_policy "
            "set numsegments = {new_cluster_size} "
            "where localoid in ({oid_list})").format(new_cluster_size=new_cluster_size,
                                                     oid_list=get_oid_list(all_parts_with_root))
    db.query(sql1)
    sql2 = ("update gp_distribution_policy "
            "set distkey = '', distclass = '' "
            "where localoid in ({oid_list})").format(oid_list=get_oid_list(childs))
    db.query(sql2)
    db.query("end;")
    db.close()

def step2_one_rel(child, db, distkey, distclass, distby):
    db.query("begin;")
    db.query("lock {relname} IN ACCESS EXCLUSIVE MODE".format(relname=child))
    sql1 = ("update gp_distribution_policy "
            "set distkey = '{distkey}', distclass = '{distclass}' "
            "where localoid = '{relname}'::regclass::oid").format(distkey=distkey,
                                                                  distclass=distclass,
                                                                  relname=child)
    db.query(sql1)
    sql2 = ("alter table {relname} set with (REORGANIZE=true) "
            "distributed by ({distby})").format(distby=distby, relname=child)
    db.query(sql2)
    db.query("end;")

def step2_worker(wid, concurrency, childs, dbname, port, host, distkey, distclass, distby):
    db = DB(dbname=dbname, host=host, port=port)
    db.query("set allow_system_table_mods = on;")
    for id, child in enumerate(childs):
        if id % concurrency == wid:
            step2_one_rel(child, db, distkey, distclass, distby)
    db.close()

def get_dist_info(relname, dbname, port, host):
    db = DB(dbname=dbname, host=host, port=port)
    sql = ("select distkey, distclass from gp_distribution_policy "
           "where localoid = '{relanme}'::regclass::oid").format(relanme=relname)
    r = db.query(sql).getresult()
    db.close()
    return r[0]

def step2(relname, childs, dbname, port, host, concurrency, distby):
    distkey, distclass = get_dist_info(relname, dbname, port, host)
    ps = []
    for i in range(concurrency):
        p = Process(target=step2_worker, args=(i, concurrency, childs,
                                               dbname, port, host,
                                               distkey, distclass, distby))
        p.start()
        ps.append(p)
    for p in ps:
        p.join()

   
if __name__ == "__main__":
    childs = get_child_of_root("rank")
    step1("rank", childs, "gpadmin", 6000, "zlyu", 4)
    step2("rank", childs, "gpadmin", 6000, "zlyu", 4, "id")
