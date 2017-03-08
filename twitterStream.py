from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import re


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)

    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    """
    pos and neg lists are used to store all the positive and negative counts per tweet
    These counts will be used for the graph plotting
    """
    posData = []
    negData = []

    for i in counts:
        posData.append(i[0][1])
        negData.append(i[1][1])
    
    timepoints = [i for i in range(len(counts))]

    posplot = plt.plot(timepoints, posData, 'bo-', label="Positive")
    negplot = plt.plot(timepoints, negData, 'go-', label="Negative")
    plt.legend(loc = 'upper left')
    plt.axis([0, len(counts), 0, max(max(posData), max(negData)) + 80])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    with open(filename, 'r') as f:
        words = [line.strip() for line in f]

    return words

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)
    
def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    
    
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total cou
    # 
    # nts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    strings = tweets.flatMap(lambda line: re.sub('\W+',' ', line).split(" "))

    wordCounts = strings.map(lambda word: ('Positive', 1) if word in pwords else ('Negative', 1) if word in nwords else None)\
                        .filter( lambda x: False if x is None else True )\
                        .reduceByKey(lambda x, y: x + y)
    
    runningCounts = wordCounts.updateStateByKey(updateFunction)
    runningCounts.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    print(counts)
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
