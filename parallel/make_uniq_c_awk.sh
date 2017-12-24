uniq_mapr="mawk -F\$'\t' -v OFS=\$'\t' 'BEGIN {gl=getline; prev_line=\$1; prev_line_field=\$2; count=\$3}
                                        { if (prev_line != \$1) {
                                              print prev_line, prev_line_field, count;
                                              prev_line=\$1; prev_line_field=\$2;
                                              count=0
                                          }
                                          count=count+\$3
                                        }
                                        END {if (gl==1) print prev_line, prev_line_field, count}'"

eval $uniq_mapr
