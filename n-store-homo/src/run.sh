
#!/bin/bash
make
#rm -fr /dev/shm/zfile
./nstore -x20 -k20 -e1 -w -y -p0.90 -u -n -M1 -I7
# rm -fr /dev/shm/zfile
#./nstore -x20000 -k10000 -e2 -w -y -p0.5 -u -i
# n = run and record log
# t = rebuild table from log
# Ux = recover on update x
# Ix = recover on insert x
# Sx = recover on select x (not implemented - kinda useless)
# M = replay up until middle of transaction. memory allocated, but transaction not commited
# 
#Macro = inbetween non-memory ops. Application-level
#Micro = memory allocation.

