# This Spark Python script loads and tokenizes a text file and defines a function to add a string to the tokens

import os.path
from pyspark import SparkContext

def addStrings(word):
    """Adds 'test' to a token word.
    
    Args:
        word (str): A string.

    Returns:
        str: A string with 'test' added to it.
    """
    return word + 'test'

if __name__ == "__main__":
    # Define file path
    baseDir = os.path.join('filedir')
    inputPath = os.path.join('data.txt')
    fileName = os.path.join(baseDir, inputPath)

    # New Spark Context
    sc = SparkContext(appName = "Tokenize and add a string to each token")

    # RDD
    data = sc.textFile(fileName)

    # Tokenize the file
    tokenWords = data.flatMap(lambda x: x.split(' ')).collect()

    # Call addStrings function on tokenWords
    testTokenWords = tokenWords.map(addStrings)
    print(testTokenWords.collect())

    sc.stop()
