from pyspark import SparkConf
from pyspark import SparkContext
import re,sys, time

word_tags = re.compile(r'^(\[(\w|\|)+\])+$')


def split_line_to_word_and_wlt(line):
    """
    This function decomposes a given line to words and words WLTs. Also marks the lemma in position 1.
    Runs through a line only once.
    :param line: string
    :return: lists of words and wlts
    """
    words = line.split(" ")
    ret = []
    for w in words:
        wlt = w.split("#")

        if w != "<s>" and w != "</s>":
            wlt[1] += "@lemma"
        ret.append(wlt)

    return words, ret


def _permute(words, words_wlt, actind, maxind, temp="", word_temp=""):
    """
    Recursively runs through the line sections.
    :param words: list of line words
    :param words_wlt: list of line wlts
    :param actind: starting place of trigram generator
    :param maxind: offset value, here the generating ends
    :param temp: builds the mosaic
    :param word_temp: builds the source n-gram
    :return:
    """
    source_ngram = '{0}{1} '.format(word_temp, words[actind])
    if actind < maxind:

        for act_wlt_ind, act_wlt in enumerate(words_wlt[actind]):
            yield from _permute(words, words_wlt, actind + 1, maxind,
                                '{0}{1} '.format(temp, act_wlt),
                                source_ngram)
        return

    # When we reach the lowest level of the recursion:
    for act_wlt in words_wlt[actind]:
        yield ('{0}{1}'.format(temp, act_wlt), (source_ngram[:-1], 1))


def permute(line):
    """
    This function generates the mosaic n-grams. Runs through a line only once.
    :param line: a line (=sentence) from the corpus
    :return: the generated n-grams (from one line)
    """
    words, words_wlt = split_line_to_word_and_wlt('<s> {0} </s>'.format(line.strip()))

    m = len(words) + 1
    n = 3

    for i in range(m - n):
        yield from _permute(words, words_wlt, i, i + n - 1)


def aggregate_by_mosaic(a, b):
    """
    :param a: (source n-gram, frequency) belonging to a mosaic n-gram
    :param b: another (source n-gram, frequency) pair belonging to the same n-gram
    :return: lexicographically smallest source n-gram, summed frequency
    """
    if b[0] < a[0]:
        return b[0], a[1] + b[1]
    else:
        return a[0], a[1] + b[1]


def mosaic_value(mosaic, val=0):
    """
    :param mosaic: string
    :param val: summed value
    :return: value after
    """
    mosaic_s = mosaic.split()
    for piece in mosaic_s:
        if word_tags.match(piece):
            val += 1
        elif re.search(r'@lemma', piece):
            val += 2
        else:
            val += 4
    return val


def remove_duplicates(a, b):
    """
    :param a: mosaic from mosaic n-gram a
    :param b: mosaic from mosaic n-gram b
    :return: whichever has a bigger mosaic_value
    """
    if mosaic_value(a) > mosaic_value(b):
        return a
    elif mosaic_value(a) == mosaic_value(b) and a<b:
        return a
    else:
        return b

if __name__ == "__main__":

    conf = SparkConf()
    conf.setMaster('spark://hadoop-master:7077')
    conf.setAppName('spark-basic')
    sc = SparkContext(conf=conf)

    host = 'hadoop-master:54310'
    text_file = sc.textFile("hdfs://" + host + "/" + sys.argv[1])

    #counts = text_file.flatMap(permute)\
    #    .reduceByKey(aggregate_by_mosaic).map(lambda x: (x[1], x[0]))\
    #    .reduceByKey(remove_duplicates).sortBy(ascending=False, keyfunc=lambda x: x[0][1])\
    #    .map(lambda x: '{0}\t{1}'.format(x[1], x[0][1]))

    #for comparing spark permute with awk permute
    #counts = text_file.flatMap(permute).map(lambda x: '{0}\t{1}'.format(x[0], x[1][0]))

    #for computing permute TIME
    #counts = text_file.flatMap(permute).map(lambda x: '{0}\t{1}'.format(x[0], x[1][0]))

    #for comparing spark aggregate_by_mosaic with awk uniq_c, as those ar equal for our purposes
    #counts = text_file.flatMap(permute).reduceByKey(aggregate_by_mosaic).map(lambda x: '{0}\t{1}\t{2}'.format(x[1][1], x[0], x[1][0]))

    #for comparing spark remove_duplicates (after a swap) and awk duplicates
    counts = text_file.flatMap(permute)\
        .reduceByKey(aggregate_by_mosaic).map(lambda x: (x[1], x[0]))\
        .reduceByKey(remove_duplicates).map(lambda x: '{0}\t{1}\t{2}'.format(x[0][0], x[0][1], x[1]))

    #for examining scores. instead of removing duplicates lines feature the freq, mosaic, score
    #counts = text_file.flatMap(permute)\
        #.reduceByKey(aggregate_by_mosaic).map(lambda x: '{0}\t{1}\t{2}\t{3}'.format(x[1][1], x[0], mosaic_value(x[0]), x[1][0]))

    output_filename = sys.argv[1] + ".out" + str(time.time())
    counts.coalesce(1).saveAsTextFile("hdfs://" + host + "/" + output_filename)
    print("created outputfile: {0}".format(output_filename)) 
    
