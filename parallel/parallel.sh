#!/bin/bash

echoerr() { echo "$@" 1>&2; }

export PWD='/common/mosaic'
export node_file='hostnames'
export corpora_name='szeged_corpus.txt'

echoerr "split(master), make_mosaic - sort - make_uniq_c(slaves) in progress"
source split_permute_freqcount.sh '20M' '70%'
echoerr "split-permute-freqcount completed"

echoerr "merge(master), make_uniq_c(master) in progress"
./mergesort_freqcount.sh '60%'
echoerr "merge1 completed"

echoerr "split(master), sort - remove_duplicates(slaves) in progress"
source split_remove_duplicates.sh '50M' '70%'
echoerr "split-remove-duplicates completed"

echoerr "merge(master), remove_duplicates(master)"
./mergesort_remove_duplicates.sh '50%'
echoerr "merge2 completed, duplicate filtered corpus is ready"

exit 0