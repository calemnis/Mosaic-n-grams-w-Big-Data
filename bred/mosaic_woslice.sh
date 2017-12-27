#! /bin/bash
# -*- coding: utf-8, vim: expandtab:ts=4 -*-

LC_ALL="C" bred -c 1 -M map -I 'awk -f' -T "-t$'\t'" -s '1,1' -r 'BEGIN {
    OFS = "\t";
    FS = "\t";
  }

  function _permute(slicearr, actind, maxind, actWLTind, oldtemp, temp){
    cWLT = split(slicearr[actind], WLT, "#");
    WLT[2] = WLT[2] "@lemma";
    oldtemp = temp;

    while (actWLTind <= cWLT && actind < maxind){
       temp = oldtemp WLT[actWLTind] " ";
       _permute(slicearr, actind + 1, maxind, 1, oldtemp, temp);
       cWLT = split(slicearr[actind], WLT, "#");
       WLT[2] = WLT[2] "@lemma";
       actWLTind += 1;
    }

    while (actWLTind <= cWLT){
        temp = oldtemp WLT[actWLTind];
        source = slicearr[1] " " slicearr[2] " " slicearr[3];
        print temp, source;
        temp = oldtemp;
        actWLTind += 1;
    }

  }
  {
    m = split("<s> " $0 " </s>", words, " ");

    for (i = 1; i<= m - 2; i++){
        slice = sep = "";
        for (k = i; k <= i + 2; k++){
            slice = (slice sep words[k])
            sep = " ";
        }
        num = split(slice, slicearr, " ");
        _permute(slicearr, 1, num, 1, "", "");

    }

  }' | pv | tee /common/bred/permute.dat | LC_ALL="C" bred -M reduce -I 'awk -f' -T "-t$'\t'" -c 1 -s '1,1nr -k3,3 -k2,2' -r 'BEGIN {

  FS = "\t";
  OFS = "\t";
  gl=getline; prev_mosaic=$1; prev_source=$2; count=1;
}
                                        { if (prev_mosaic != $1) {
                                              print count, prev_mosaic, prev_source;
                                              prev_mosaic=$1; prev_source=$2;
                                              count=0;
                                          }
                                          count++;
                                        }
END {if (gl==1) print count, prev_mosaic, prev_source;}' | pv | tee /common/bred/uniq.dat | bred -M reduce -I 'awk -f' -c 1 -O no -r 'function scoreMosaic(mosaic){
                                                  sum = 0;
                                                  c = split(mosaic, tokens, " ");
                                                  for (i=1; i<=c; ++i)
                                                      if (tokens[i] ~ /^\[[^ ]+\]$/) {sum += 1}
                                                      else if (match(tokens[i], /@lemma/)) {sum += 2}
                                                      else sum += 4;
                                                  return sum
                                              }
                                              function printOut(){
                                                  print oldFreq, maxMosaic, oldNgram;
                                                  maxMosaic = actMosaic;
                                                  maxScore = scoreMosaic(actMosaic);
                                                  oldFreq = actFreq;
                                                  oldNgram = actNgram
                                              }
                                              BEGIN {
                                                     FS = "\t";
                                                     OFS = "\t";
                                                     gl=getline;
                                                     if (gl == 1) {
                                                         oldFreq = $1;
                                                         maxMosaic = $2;
                                                         oldNgram = $3;
                                                         maxScore = scoreMosaic(maxMosaic)
                                                     }
                                              }

                                              { actFreq = $1;
                                                actMosaic = $2;
                                                actNgram = $3;
                                                if (actFreq == oldFreq && actNgram == oldNgram){
                                                    actScore = scoreMosaic(actMosaic);
                                                    if (actScore > maxScore){
                                                        maxScore = actScore;
                                                        maxMosaic = actMosaic
                                                    }
                                                }
                                                else printOut()
                                              }
                                              END{if (gl == 1) printOut()}' | pv | tee /common/bred/duplicates.dat
