#!/bin/sh

file="/home/lpf/workspace/mydestor/log"

if [ $# -gt 0 ]; then
	file=$1
fi
echo "split file -> $file"

> ./result/read_phase.log
file_chunk_num=0
file_total_num=0
read_phase="Read phase"
read_chunk="Read phase: read"

> ./result/chunk_phase.log
chunk_num=0
zero_chunk_num=0
chunk_phase="Chunk phase"
chunk_zero="zero"

> ./result/hash_phase.log
hash_chunk_num=0
hash_phase="Hash phase"

> ./result/dedup_phase.log
dedup_phase="Dedup phase"

> ./result/filter_phase.log
filter_phase="Filter phase"

> ./result/lpf_phase.log
lpf_phase="LPF"
lpf_num=0

while read line
do
	# the log of read_phase
	if [[ $line == $read_phase* ]]; then
		echo $line >> ./result/read_phase.log
		file_total_num=$(($file_total_num+1))
		
		if [[ $line == $read_chunk* ]]; then
			file_chunk_num=$(($file_chunk_num+1))
		fi
	fi

	# the log of chunk_phase
	if [[ $line == $chunk_phase* ]]; then
		echo $line >> ./result/chunk_phase.log
		chunk_num=$(($chunk_num+1))
		
		if [[ $line == *$chunk_zero* ]]; then
			zero_chunk_num=$(($zero_chunk_num+1))
		fi
	fi

	# the log of hash_phase
	if [[ $line == $hash_phase* ]]; then
		echo $line >> ./result/hash_phase.log
		hash_chunk_num=$(($hash_chunk_num+1))
	fi

	# the log of dedup_phase
	if [[ $line == $dedup_phase* ]]; then
		echo $line >> ./result/dedup_phase.log
	fi

	# the log of filter_phase
	if [[ $line == $filter_phase* ]]; then
		echo $line >> ./result/filter_phase.log
	fi

	# the log of lpf_phase
	if [[ $line == $lpf_phase* ]]; then
		echo $line >> ./result/lpf_phase.log
		lpf_num=$(($lpf_num+1))
	fi
	
done < $file

file_num=$[file_total_num-file_chunk_num]
echo "file_num -> $file_num" >> ./result/read_phase.log
echo "file_chunk_num -> $file_chunk_num" >> ./result/read_phase.log
echo "file_total_num -> $file_total_num" >> ./result/read_phase.log

echo "chunk_num -> $chunk_num" >>  ./result/chunk_phase.log
echo "zero_chunk_num -> $zero_chunk_num" >> ./result/chunk_phase.log

echo "hash_chunk_num -> $hash_chunk_num" >> ./result/hash_phase.log

echo "lpf_num -> $lpf_num" >> ./result/lpf_phase.log

echo "split file finished"

sed -e '/Read phase/d;/Chunk phase/d;/Hash phase/d;/Dedup phase/d;/Filter phase/d;/LPF/d' $file > ./result/only_output.log
