##!/bin/zsh

cwd=$(pwd)
rm -rf build || true
go build -o ./build/bin/maelstrom
~/Repository/maelstrom/maelstrom test -w kafka --bin $cwd/build/bin/maelstrom --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
cd $cwd
