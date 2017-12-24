#!/bin/bash

SPLIT_SIZE=$1
SORT_BUFFER=$2

pbzip2 -cd ${corpora_name%.*}_merged.bz2 | split -d -C $SPLIT_SIZE --filter='pbzip2 > $FILE.bz2' - ${corpora_name%.*}_reduced_splitted_ 
#corpora_name_reduced_splitted_01.bz2

for file in ${corpora_name%.*}_reduced_splitted*
do
    echo "pbzip2 -cd ${file} | ./make_reorder_sort_remove_duplicates.sh $SORT_BUFFER | pbzip2 > filtered_${file%.*}.bz2" #filtered_corpora_name_reduced_splitted_01.bz2
done | tee commands2.txt

parallel --jobs 1 --no-notice --sshloginfile $node_file --workdir $PWD < commands2.txt

dupl_num=$(ls -l filtered_${corpora_name%.*}* | wc -l)