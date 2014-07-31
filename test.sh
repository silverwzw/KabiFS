#!/bin/sh
cd $(dirname $(readlink -f $0)) # delete this line if error pops up
./compile.sh && sudo ./mount.sh examples.json ./mnt && chmod 777 mnt && cp -r src mnt/ && diff src mnt/src && ./unmnt.sh
