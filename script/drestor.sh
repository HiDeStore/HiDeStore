#!/bin/bash


dataset="kernel"

kernel_path="/home/lpf/workspace/kernel_data_debug/"
#kernel_path="/home/lpf/workspace/kernel/kernel"
gcc_path="/home/lpf/workspace/gcc/gcc"
fsl_path="/home/lpf/workspace/fslhomes/fsl/"
macos_path="/home/lpf/workspace/macos/macos"


case $dataset in
	"kernel")
		path=$kernel_path
		;;
	"gcc")
		path=$gcc_path
		;;
	"fsl")
		path=$fsl_path
		;;
	"macos")
		path=$macos_path
		;;
	*)
		echo "Wrong dataset!"
		exit 1
		;;
esac


DIR=/home/lpf/workspace/restore/
rm -rf ${DIR}/*

> drlog
for ((i=0;i<10;i++))
do
	echo "restore job ${i}"
	mkdir -p ${DIR}/destor
	./build/destor -r${i} ${DIR}/destor >> drlog
	echo " " >> drlog
done
