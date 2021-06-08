```
gpadmin@zlyu:~/expand_leafs$ python expand_part.py -h
usage: expand_part.py [-h] [--root ROOT] [--njobs NJOBS] [--newsize NEWSIZE]
                      [--distby DISTBY] [--dbname DBNAME] [--host HOST]
                      [--port PORT]

Expand leafs one by one

optional arguments:
  -h, --help         show this help message and exit
  --root ROOT        root partition name
  --njobs NJOBS      number of concurrent leafs to expand at the same time
  --newsize NEWSIZE  cluster size after expansion
  --distby DISTBY    root table distby clause, like "c1, c2"
  --dbname DBNAME    database name to connect
  --host HOST        hostname to connect
  --port PORT        port to connect
  ```