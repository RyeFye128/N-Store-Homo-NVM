
#!/bin/bash
make
rm -fr /dev/shm/zfile
./nstore -x20 -k10 -e1 -w -y -p0.5 -u
# rm -fr /dev/shm/zfile
#./nstore -x20000 -k10000 -e2 -w -y -p0.5 -u -i
