#!/bin/bash

SORT_BUFFER=$1
input_files='reduced_'${corpora_name%.*}'_splitted'

batch_size=$(($splitted_num+5))

for file in $input_files*
do
    var=$var"<(pbzip2 -cd $file) "
done


command="LC_ALL=C sort --merge --compress-program=pbzip2 --parallel=$(nproc) -S $SORT_BUFFER --batch-size=$batch_size $var"

eval $command | pv | ./make_uniq_c_awk.sh | pbzip2 > ${corpora_name%.*}_merged.bz2