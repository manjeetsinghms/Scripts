# This Spark Python script loads a file, removes punctuation, changes to lower case, and strips leading and trailing spaces. Word Count is printed.

import os.path
from pyspark import SparkContext

def cleanStrings(word):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.
    
    Args:
        word (str): A string.

    Returns:
        str: Clean string.
    """
    return re.sub(r'[^\sa-zA-Z0-9]', '', text).lower().strip()

if __name__ == "__main__":
    # Define file path
    baseDir = os.path.join('filedir')
    inputPath = os.path.join('data.txt')
    fileName = os.path.join(baseDir, inputPath)

    # New Spark Context
    sc = SparkContext(appName = "fileCleanupWordCount")

    # Clean RDD
    data = (sc.textFile(fileName).map(cleanStrings))

    # Tokenize the file
    tokenWords = data.flatMap(lambda x: x.split(' '))

    # Word Count
    wordCount = tokenWords.map(lambda x: (x,1).reduceByKey(lambda a,b: a+b)).collect()
    print(wordCount)

    sc.stop()
