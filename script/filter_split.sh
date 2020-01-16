#!/bin/sh

file="/home/lpf/workspace/mydestor/result/filter_phase.log"
echo "split file -> $file"

out_file="./result/filter/out_of_order.log"
> $out_file
out_of_order="out-of-order"
out_num=0

check_file="./result/filter/index_check_buffer.log"
> $check_file
index_check_buffer="index_check_buffer"
index_check_buffer_num=0

update_file="./result/filter/update_index.log"
> $update_file
update_index="update"
update_index_num=0

denied_file="./result/filter/denied.log"
> $denied_file
denied="denied"
denied_num=0

while read line
do
	# out_of_order
	if [[ $line == *$out_of_order* ]]; then
		echo $line >> $out_file
		out_num=$(($out_num+1))
	fi

	# index_check_buffer
	if [[ $line == *$index_check_buffer* ]]; then
		echo $line >> $check_file
		index_check_buffer_num=$(($index_check_buffer_num+1))
	fi

	# update_index
	if [[ $line == *$update_index* ]]; then
		echo $line >> $update_file
		update_index_num=$(($update_index_unm+1))
	fi

	# denied
	if [[ $line == *$denied* ]]; then
		echo $line >> $denied_file
		denied_num=$(($denied_num+1))
	fi

done < $file

echo "out_num: $out_num" >> $out_file
echo "index_check_buffer_num: $index_check_buffer_num" >> $check_file
echo "update_index_num: $update_index_num" >> $update_file
echo "denied_num: $denied_num" >> $denied_file

sed "/out-of-order/d;/index_check_buffer/d;/update/d;/denied/d" $file > ./result/filter/only_filter.log
