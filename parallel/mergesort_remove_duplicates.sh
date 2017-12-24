remove_duplicate="mawk -F $'\t' -v OFS=$'\t' 'function scoreMosaic(mosaic){
                                                  sum = 0;
                                                  c = split(mosaic, tokens, \" \");
                                                  for (i=1; i<=c; ++i)
                                                      if (tokens[i] ~ /^\[[^ ]+\]\$/) {sum += 1}
                                                      else if (index(tokens[i], \"@lemma\") >= 1) {sum += 2}
                                                      else sum += 4;
                                                  return sum
                                              }
                                              function printOut(){
                                                  print oldFreq, oldNgram, maxMosaic;
                                                  maxMosaic = actMosaic;
                                                  maxScore = scoreMosaic(actMosaic);
                                                  oldFreq = actFreq;
                                                  oldNgram = actNgram
                                              }
                                              BEGIN {gl=getline;
                                                     if (gl == 1) {
                                                         oldFreq = \$1;
                                                         maxMosaic = \$3;
                                                         oldNgram = \$2;
                                                         maxScore = scoreMosaic(maxMosaic)
                                                     }
                                              }
                                              { actFreq = \$1;
                                                actMosaic = \$3;
                                                actNgram = \$2;
                                                if (actFreq == oldFreq && actNgram == oldNgram){
                                                    actScore = scoreMosaic(actMosaic);
                                                    if (maxScore < actScore){
                                                        maxScore = actScore;
                                                        maxMosaic = actMosaic
                                                    }
                                                }
                                                else printOut()
                                              }
                                              END{if (gl == 1) printOut()}'"

SORT_BUFFER=$1
batch_size=$(($dupl_num+5))
input_files='filtered_'${corpora_name%.*}'_reduced_splitted'

for file in $input_files*
do
    var=$var"<(pbzip2 -cd $file) "
done


command="LC_ALL=C sort --merge --compress-program=pbzip2 --parallel=$(nproc) -S $SORT_BUFFER --batch-size=$batch_size $var"

eval $command | pv | eval $remove_duplicate | pbzip2 > ${corpora_name%.*}_filtered_merged_ready.bz2
