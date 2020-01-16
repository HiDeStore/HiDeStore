#!/bin/bash

dataset="kernel"

kernel_path="/home/lpf/workspace/kernel_data_debug/"
#kernel_path="/home/lpf/workspace/kerneldata/"

path=$kernel_path

./rebuild
> log
./build/destor $path > log
#for file in $(ls $path); do
#	./build/destor $path/$file >> log
#done

./script/split.sh
./script/dedup_split.sh
./script/filter_split.sh
