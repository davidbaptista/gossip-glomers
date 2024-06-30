##!/bin/zsh

cwd=$(pwd)
rm -rf build || true
go build -o ./build/bin/maelstrom
~/Repository/maelstrom/maelstrom test -w g-counter --bin $cwd/build/bin/maelstrom  --node-count 25 --time-limit 20 --rate 100 --latency 100
cd $cwd
