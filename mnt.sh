#!/bin/sh
cd $(dirname $(readlink -f $0)) # delete this line if error pops up
cp ./log4j/log4j.properties ./bin/log4j.properties
java	-cp ./bin:./lib/* \
	com.silverwzw.kabiFS.Mount "$@" &
