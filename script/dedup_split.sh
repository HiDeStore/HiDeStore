#!/bin/sh
file="/home/lpf/workspace/mydestor/result/dedup_phase.log"
echo "split file -> $file"

> ./result/dedup/non_existing.log
non_existing="non-existing"
non_num=0

> ./result/dedup/unique.log
unique="chunk is unique"
unique_num=0

> ./result/dedup/storage_buffer.log
storage_buffer="storage_buffer"
storage_num=0

> ./result/dedup/container.log
container="container"
container_num=0

> ./result/dedup/cache.log
cache="cache"
cache_num=0

> ./result/dedup/index_buffer.log
index_buffer="index_buffer"
index_num=0
recent_dedup=0

> ./result/dedup/iden_unique.log
iden_unique="to a unique chunk"
iden_num=0

htable="htable"
htable_num=0

while read line
do
	# htable
	if [[ $line == *$htable* ]]; then
		htable_num=$(($htable_num+1))
	fi

	# non-existing fingerprint
	if [[ $line == *$non_existing* ]]; then
		echo $line >> ./result/dedup/non_existing.log
		non_num=$(($non_num+1))
	fi

	# chunk is unique
	if [[ $line == *$unique* ]]; then
		echo $line >> ./result/dedup/unique.log
		unique_num=$(($unique_num+1))
	fi
	
	# storage_buffer
	if [[ $line == *$storage_buffer* ]]; then
		echo $line >> ./result/dedup/storage_buffer.log
		storage_num=$(($storage_num+1))
	fi

	# container
	if [[ $line == *$container* ]]; then
		echo $line >> ./result/dedup/container.log
		container_num=$(($container_num+1))
	fi

	# cache
	if [[ $line == *$cache* ]]; then
		echo $line >> ./result/dedup/cache.log
		cache_num=$(($cache_num+1))
	fi
	
	# index_buffer
	if [[ $line == *$index_buffer* ]]; then
		echo $line >> ./result/dedup/index_buffer.log
		index_num=$(($index_num+1))
		if [[ $line == *-1* ]]; then
			recent_dedup=$(($recent_dedup+1))
		fi
	fi

	# identical to a unique chunk
	if [[ $line == *$iden_unique* ]]; then
		echo $line >> ./result/dedup/iden_unique.log
		iden_num=$(($iden_num+1))
	fi
done < $file

echo "non_existing fingerprint: $non_num" >> ./result/dedup/non_existing.log
echo "chunk is unique: $unique_num" >> ./result/dedup/unique.log
echo "storage_buffer: $storage_num" >> ./result/dedup/storage_buffer.log
echo "container_num: $container_num" >> ./result/dedup/container.log
echo "cache_num: $cache_num" >> ./result/dedup/cache.log
echo "index_num: $index_num" >> ./result/dedup/index_buffer.log
echo "recent_dedup_num: $recent_dedup" >> ./result/dedup/index_buffer.log
echo "iden_num: $iden_num" >> ./result/dedup/iden_unique.log

# the summary file
summary_total=$[storage_num+index_num+cache_num+non_num+htable_num]
summary_file="./result/dedup/summary.log"
echo "the chunks in index_lookup_base():" > $summary_file
printf "%-25s %-3s %-15d\n" storage_buffer : $storage_num >> $summary_file
printf "%-25s %-3s %-15d\n" index_buffer : $index_num >> $summary_file
printf "%-25s %-3s %-15d\n" cache : $cache_num >> $summary_file
printf "%-25s %-3s %-15d\n" non-esisting : $non_num >> $summary_file
printf "%-25s %-3s %-15d\n" htable : $htable_num >> $summary_file
printf "%-25s %-3s %-15d\n" total : $summary_total >> $summary_file

send_segment_total=$[iden_num+container_num+unique_num]
printf "\nthe chunks in send_segment():\n" >> $summary_file
printf "%-25s %-3s %-15d\n" identical_to_unique : $iden_num >> $summary_file
printf "%-25s %-3s %-15d\n" identical_to_container : $container_num >> $summary_file
printf "%-25s %-3s %-15d\n" chunk_is_unique : $unique_num >> $summary_file
printf "%-25s %-3s %-15d\n" total : $send_segment_total >> $summary_file

sed "/non-existing/d;/chunk is unique/d;/storage_buffer/d;/container/d;/cache/d" $file > ./result/dedup/only_dedup.log
