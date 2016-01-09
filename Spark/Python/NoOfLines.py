# This Spark Python script loads a text file and prints number of lines

import os.path
from pyspark import SparkContext

if __name__ == "__main__":
    # Define file path
    baseDir = os.path.join('filedir')
    inputPath = os.path.join('data.txt')
    fileName = os.path.join(baseDir, inputPath)

    # New Spark Context
    sc = SparkContext(appName = "LineCount")

    # RDD
    data = sc.textFile(fileName)
    lines = rawData.count()

    print(lines)
    sc.stop()

