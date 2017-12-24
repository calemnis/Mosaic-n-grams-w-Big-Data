#!/bin/bash

SPLIT_SIZE=$1
SORT_BUFFER=$2

cat $corpora_name | pv | split -d -a4 -C $SPLIT_SIZE --filter='pbzip2 > $FILE.bz2' - ${corpora_name%.*}_splitted. #corpora_name_splitted.0000.bz2

for file in ${corpora_name%.*}_splitted*
do
    echo "pbzip2 -cd ${file} | parallel --no-notice --pipe -j\$(nproc) --compress ./make_mosaic_awk.sh |\
 LC_ALL=C sort --parallel=\$(nproc) --compress-program=pbzip2 -S $SORT_BUFFER | ./make_uniq_c_awk.sh |\
 pbzip2 > reduced_${file%.*}.bz2" #reduced_corpora_name_splitted.0000.bz2
done | tee commands.txt

parallel --jobs 1 --no-notice --sshloginfile $node_file --workdir $PWD < commands.txt

splitted_num=$(ls -l reduced_${corpora_name%.*}* | wc -l)
