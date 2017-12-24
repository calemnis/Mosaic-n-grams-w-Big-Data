mosaic_ngram_3_mapr="mawk -v maxn=3 -v delimit='#' 'function permute(words, actind, maxind, actWLTind, oldtemp, temp, oldWordtemp, wordtemp) {
                                                                                                                cWLT = split(words[actind], WLT, delimit);
                                                                                                                WLT[2] = WLT[2] \"@lemma\";
                                                                                                                oldtemp = temp;
                                                                                                                oldWordtemp = wordtemp;

                                                                                                                while (actWLTind <= cWLT && actind < maxind){
                                                                                                                        temp = oldtemp WLT[actWLTind] \" \";
                                                                                                                        wordtemp = oldWordtemp words[actind] \" \";
                                                                                                                        permute(words, actind+1, maxind, 1, oldtemp, temp, oldWordtemp, wordtemp);
                                                                                                                        cWLT = split(words[actind], WLT, delimit);
                                                                                                                        WLT[2] = WLT[2] \"@lemma\";
                                                                                                                        actWLTind += 1
                                                                                                                };

                                                                                                                while (actWLTind <= cWLT){
                                                                                                                        temp = oldtemp WLT[actWLTind];
                                                                                                                        wordtemp = oldWordtemp words[actind];
                                                                                                                        print temp \"\t\" wordtemp \"\t\" 1;
                                                                                                                        temp = oldtemp;
                                                                                                                        wordtemp = oldWordtemp;
                                                                                                                        actWLTind += 1
                                                                                                                }

                                                                                                         }
                                                                                                         {min=maxn<NF?maxn:NF;
                                                                                                          m=NF+1;
                                                                                                          n=maxn;
                                                                                                          for (i=1; i <= m - n + 2; i++) {
                                                                                                                  split(\"<s> \" \$0 \" </s>\", words);
                                                                                                                  permute(words, i, i + n - 1, 1, \"\", \"\", \"\", \"\")
                                                                                                          }
                                                                                                         }'"

eval $mosaic_ngram_3_mapr
