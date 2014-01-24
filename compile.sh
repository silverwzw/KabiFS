#!/bin/bash
gcc -Wall kabifs.c `pkg-config fuse --cflags --libs` -o kabi
