## Overview

1. Use expand_part.py to finish the data expansion.
2. Use fix_policy_after_rebalance_data.py to fix catalog `gp_distribution_policy` inconsistent.

## expand_part.py

```
gpadmin@zlyu:~/expand_leafs$ python expand_part.py -h
usage: expand_part.py [-h] --root ROOT --njobs NJOBS --newsize NEWSIZE
                      --distby DISTBY --dbname DBNAME --host HOST
                      [--childrenfile CHILDRENFILE] --port PORT --user USER
                      --log LOG

Expand leafs one by one

optional arguments:
  -h, --help            show this help message and exit
  --root ROOT           root partition name (fully qualified)
  --njobs NJOBS         number of concurrent leafs to expand at the same time
  --newsize NEWSIZE     cluster size after expansion
  --distby DISTBY       root table distby clause, like "c1, c2"
  --dbname DBNAME       database name to connect
  --host HOST           hostname to connect
  --childrenfile CHILDRENFILE
                        file containing fully qualified child partition names
  --port PORT           port to connect
  --user USER           username to connect with
  --log LOG             log file path
```

-------------

## fix_policy_after_rebalance_data.py

```
gpadmin@zlv-ubuntu:~$ python fix_policy_after_rebalance_data.py -h
usage: fix_policy_after_rebalance_data.py [-h] --root_oid ROOT_OID --newsize
                                          NEWSIZE --dbname DBNAME --host HOST
                                          --port PORT --user USER --log LOG

Expand leafs one by one

optional arguments:
  -h, --help           show this help message and exit
  --root_oid ROOT_OID  root partition oid
  --newsize NEWSIZE    cluster size after expansion
  --dbname DBNAME      database name to connect
  --host HOST          hostname to connect
  --port PORT          port to connect
  --user USER          username to connect with
  --log LOG            log file path
```