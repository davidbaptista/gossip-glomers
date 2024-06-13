#!/bin/bash

cwd=$(pwd)
rm -rf build || true
go build -o ./build/bin/maelstrom
~/Repository/maelstrom/maelstrom test -w broadcast --bin $cwd/build/bin/maelstrom --node-count 5 --time-limit 20 --rate 10 --nemesis partition
cd $cwd
