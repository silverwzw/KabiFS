#!/bin/sh
javac	-cp "./lib/*" \
	-sourcepath ./src:./fuse-jna \
	-d ./bin \
	./src/com/silverwzw/kabiFS/KabiFS.java
