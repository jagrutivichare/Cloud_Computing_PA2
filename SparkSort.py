from pyspark import SparkContext
import sys

if(len(sys.argv)<2):
    print "Missing arguments"
    sys.exit(1)

sc = SparkContext("local","Spark Sort")
inputfile = sys.argv[1]
outputfile = sys.argv[2]
textinput = sc.textFile(inputfile)

lines = textinput.flatMap(lambda line: line.split("\n"))
records = lines.map(lambda text: str(text[:10]),str(text[[10:]]))
sortedRecords = records.sortByKey()

sortedRecords.saveAsTextFile(outputfile)