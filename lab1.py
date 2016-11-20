import binascii
import random
import sys
from datetime import datetime

from pyspark import SparkConf, SparkContext

bad_characters = ['.', ',', ";", ":", "!", "?", "\"", "\'", "(", ")"]

#Tested
def old_line_hashed_shingle(tokens, k):
	arr = []
	if len(tokens)%2 == 1:
		max_i = len(tokens) - k
	else:
		max_i = len(tokens) - k + 1

	for i in range(max_i + 1):
		#arr.append(binascii.crc32(' '.join(tokens[i:i+k])))
		arr.append(' '.join(tokens[i:i+k]))

	return arr

#Tested
def line_hashed_shingle(token, k):
	arr = []
	if len(token) < k:
		return False
	for i in range(len(token) - k + 1):
		arr.append(binascii.crc32(token[i:i+k]))

	return arr

#Tested
def make_hashed_shingles(textFile, k):
	#import pdb; pdb.set_trace()
	arr = []
	size = textFile.count()
	bff = ''

	for i in range(size):
		line = textFile.filter(lambda (x, y) : y == i).map(lambda (x, y) : x).collect()[0]
		line = bff + line
		hashed_shingles = line_hashed_shingle(line, k)
		arr = arr + hashed_shingles if hashed_shingles != False else arr
		bff = line[-(k-1):]
		bff = bff + ' ' if bff != '' else ''

	return arr

class Shingling(object):

	def __init__(self, sc, filename, k):
		self.k = k
		self.filename = filename
		self.sc = sc

		#conf = SparkConf().setAppName("building a warehouse")
		#sc = SparkContext(conf=conf)

		textFile = sc.textFile(filename)
		#textFile = textFile.map(lambda x: x.split()).zipWithIndex()

		#Remove bad characters
		textFile = textFile.map(lambda x: x.translate(''.join(bad_characters)))

		#Add index to lines
		textFile = textFile.zipWithIndex()
		self.arr = make_hashed_shingles(textFile, k)


	def __str__(self):
		return self.filename + ": " + self.k + "-Shingle"

	def print_all(self):
		print self.arr


	def get_jaccard_similarity(self, obj):
		dataset1 = self.sc.parallelize(self.arr)
		dataset2 = self.sc.parallelize(obj.arr)

		return float(dataset1.intersection(dataset2).distinct().count())/dataset1.union(dataset2).distinct().count()



class minHashMatrix(object):

	def __init__(self, sc, shingling1, shingling2, hash_functions):
		random.seed(datetime.now())

		self.sc = sc

		dataset1 = self.sc.parallelize(shingling1.arr)
		dataset2 = self.sc.parallelize(shingling2.arr)

		dataset1.cache()
		dataset2.cache()

		dataset_union = dataset1.union(dataset2).distinct().zipWithIndex()

		self.hash_functions = hash_functions

		self.minhash_arr1 = []
		self.minhash_arr2 = []

		for i in range(hash_functions):
			#Make hash function uniformly
			print "hash_func: " + str(i) 
			hash_func = [random.randint(0, dataset_union.count()-1) for x in range(dataset_union.count())]
		
			#Add "infinite element" to arr1 and arr2
			self.minhash_arr1.append(sys.maxint)
			self.minhash_arr2.append(sys.maxint)
			

			#Go through all elements in M(dataset_union)
			for j in range(dataset_union.count()):

				#Get element of the current row j
				elem = dataset_union.filter(lambda (x, y) : y == j).map(lambda (x, y) : x).collect()[0]
				#import pdb; pdb.set_trace()
				#check if M(j,0) is 1 and add the value of the hash function
				if dataset1.filter(lambda x : x == elem).count() >= 1 and self.minhash_arr1[i] > hash_func[j]:
					self.minhash_arr1[i] = hash_func[j]


				#do the same for dataset1
				if dataset2.filter(lambda x : x == elem).count() >= 1 and self.minhash_arr2[i] > hash_func[j]:
					self.minhash_arr2[i] = hash_func[j]

	def get1(self):
		return self.minhash_arr1

	def get2(self):
		return self.minhash_arr2

	def print_all(self):
		print self.minhash_arr1
		print self.minhash_arr2


	def __str__(self):
		return "Min Hash Matrix (" + self.hash_functions + " hash functions)"
















