#**********************
#*
#* Progam Name: MP1. Membership Protocol.
#*
#* copy of run.sh
#* this script is for running different dev veriosn of original code.
#* 
#***********************
#!/bin/sh

make

./Application testcases/singlefailure.conf >dbg.0.log 
./Application testcases/multifailure.conf >dbg.1.log
./Application testcases/msgdropsinglefailure.conf >dbg.2.log
