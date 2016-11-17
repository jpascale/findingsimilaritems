import binascii

from pyspark import SparkConf, SparkContext

#Tested
def line_hashed_shingle(tokens, k):
	arr = []
	if len(tokens)%2 == 1:
		max_i = len(tokens) - k
	else:
		max_i = len(tokens) - k + 1

	for i in range(max_i):
		arr.append(binascii.crc32(' '.join(tokens[i:i+k])))

	return arr

#Tested
def make_hashed_shingles(textFile, k):
	#import pdb; pdb.set_trace()
	arr = []
	size = textFile.count()
	bff = []

	for i in range(size):
		line = textFile.filter(lambda (x, y) : y == i).map(lambda (x,y) : x).collect()[0]
		line = bff + line
		arr = arr + line_hashed_shingle(line, k)
		bff = line[-(k-1):]

	return arr

class Shingling(object):

	def __init__(self, sc, filename, k):
		self.k = k
		self.filename = filename

		#conf = SparkConf().setAppName("building a warehouse")
		#sc = SparkContext(conf=conf)

		textFile = sc.textFile(filename)
		textFile = textFile.map(lambda x: x.split()).zipWithIndex()
		self.arr = make_hashed_shingles(textFile, k)


	def __str__(self):
		return self.filename + ": " + self.k + "-Shingle"

	def print_all(self):
		print self.arr