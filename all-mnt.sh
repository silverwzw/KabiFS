#!/bin/sh
cp ./log4j/all-log4j.properties ./bin/log4j.properties
java	-cp ./bin:./lib/* \
	com.silverwzw.kabiFS.KabiFS "$@" &
