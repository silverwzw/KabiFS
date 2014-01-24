#!/bin/bash
gcc -Wall kabi.c `pkg-config fuse --cflags --libs` -o kabi
