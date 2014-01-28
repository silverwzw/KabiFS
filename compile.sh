#!/bin/sh
javac	-cp "./lib/*" \
	-sourcepath ./src:./fuse-jna:./json \
	-d ./bin \
	./src/com/silverwzw/kabiFS/KabiFS.java
