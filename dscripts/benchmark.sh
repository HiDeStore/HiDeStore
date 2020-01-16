#!/bin/bash

dataset="kernel"

if [ $# -gt 0 ];then
    echo "dataset <- $1"
    dataset=$1
else
    echo "default dataset <- $dataset"
fi


kernel_path="/home/lpf/workspace/kernel_data_debug/"
gcc_path="/home/lpf/workspace/gcc/gcc"
#fsl_path="/home/lpf/workspace/fslhomes/fsl/"
#macos_path="/home/lpf/workspace/macos/macos"

#kernel_rcs=(4 8 16 32 64 128 256 )
kernel_rcs=(4)
gcc_rcs=(64 128 256 512 1024 2048 4096)


# path: where trace files locate
# rcs: the restore cache size
case $dataset in
    "kernel") 
        path=$kernel_path
        rcs=(${kernel_rcs[@]})
        ;;
    "vmdk")
        path=$gcc_path
        gcc=(${gcc_rcs[@]})
        ;;
    *) 
        echo "Wrong dataset!"
        exit 1
        ;;
esac

# ./rebuild would clear data of previous experiments
# ./destor executes a backup job
#   (results are written to backup.log)
# ./destor -rN executes a restore job under various restore cache size
#   (results are written to restore.log)

n=0
./rebuild
> deduplog
> restorelog
for file in $(ls $path);do
    ./build/destor $path/$file >> deduplog
#    for s in ${rcs[@]};do
#        ./build/destor -r$n /home/lpf/workspace/restore -p"restore-cache lru $s" >> restorelog
#        ./build/destor -r$n /home/lpf/workspace/restore -p"restore-cache opt $s" >> restorelog
#    done
#    n=$(($n+1))
done
./build/destor -s >> backup.log

# split the restore.log according to the restore cache size
split_file(){
    lines=$(cat $1) # read the file
    IFS=$'\n' # split 'lines' by '\n'
    lineno=0
    for line in $lines; do
        index=$(( ($lineno/2)%${#rcs[@]} ))
        if [ $(($lineno%2)) -eq 0 ];then
            echo $line >> restore.lru${rcs[$index]}.log
        else
            echo $line >> restore.opt${rcs[$index]}.log
        fi
        lineno=$(($lineno+1))
    done
}

split_file restore.log
