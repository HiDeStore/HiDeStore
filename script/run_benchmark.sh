#!/bin/bash


dataset="kernel"

if [ $# -gt 0 ];then
    echo "dataset <- $1"
    dataset=$1
else
    echo "default dataset <- $dataset"
fi


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

hashfile=".4kb.hash.anon"

./rebuild
> log

if [[ "$dataset" = "kernel" ]] || [[ "$dataset" = "gcc" ]]; then
	for file in $(ls $path); do
		./build/destor $path/$file -a>> log
		echo " " >> log
		echo "process $file"
	done
fi

if [[ "$dataset" = "fsl" ]] || [[ "$dataset" = "macos" ]]; then
	for dir in $(ls $path); do
		for file in $(ls $path/$dir); do
			if [[ $file == *$hashfile* ]]; then
				./build/destor $path/$dir/$file -a -p"simulation-level all" -p"trace-format fsl">> log
				echo " " >> log
				echo "process $file"
			fi
		done
	done
fi

./build/destor -s >> backup.log