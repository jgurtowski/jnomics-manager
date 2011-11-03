#!/usr/bin/env python

import os
import sys
import math
import optparse
from optparse import OptionParser

'''
A simple tool that scans some or all of a SAM-formatted file and calculates the fragment length average and standard deviation
Creation date: August 18, 2011
Revision number: 793 
Last edit date: Wed Sep 14 19:42:53 EDT 2011

@author: Matthew Titmus
'''

__author__ = "Matthew A. Titmus"
__copyright__ = "Copyright 2011"
__credits__ = ["Matthew Titmus"]
__license__ = "GPL"
__version__ = "0.1-r793"
__email__ = "mtitmus@cshl.edu"
__status__ = "Development"

def _ensureCapacity(lengths, minCapacity):
	currentCapacity = len(lengths)

	if currentCapacity < minCapacity:
		for i in range(currentCapacity, minCapacity):
			lengths.append(0)


def doCounts(inFile, maxLines=0, maxLength=0, roundDigits=2):
	lengthCounts = []
	_ensureCapacity(lengthCounts, 300)

	if maxLength <= 0:
		maxLength = sys.maxint

	if inFile == "stdin":
		instream = sys.stdin
	elif os.path.exists(inFile):
		instream = open(inFile, 'r')
	else:
		sys.stderr.write(sys.argv[0].split('/')[-1] + ": no such file: " + inFile)
		sys.exit(1)

	readCount = 0
	lengthsSum = 0
	countDroppedForLength = 0

	line = getline(instream)
	while line != '' and readCount < maxLines:
		splode = line.rstrip().split('\t')

		if len(splode) < 11:
			sys.stderr.write(inFile + ": line starting with \"" + line[0:40] + "...\" contains " + str(len(splode)) + " column(s). Is it a SAM file?")
			sys.exit(3)

		# SAM column 9 contains the observed template length 
		templateLength = int(splode[8])

		if templateLength > 0:
			if templateLength > maxLength:
				countDroppedForLength += 1
			else:
				if templateLength >= len(lengthCounts):
					_ensureCapacity(lengthCounts, templateLength + 1)

				lengthsSum += int(templateLength)
				lengthCounts[templateLength] += 1
				readCount += 1

		line = getline(instream)
	# End: while line != '' and readCount < maxLines

	if readCount == 0:
		print "Error: No reads found in input."
		sys.exit(1)

	mean = float(lengthsSum) / float(readCount)

    # We break if we go for 10000 positions without encountering a value != 0
	noValuesCounter = 0
	noValuesLimit = 10000
	lengthSquaresSum = 0.0
	i = 0
	for i in range(len(lengthCounts)):
		if lengthCounts[i] == 0:
			noValuesCounter += 1
			if noValuesCounter >= noValuesLimit:
				lengthCounts = lengthCounts[:i - noValuesCounter]
				break
		else:
			noValuesCounter = 0
			lengthCount = float(lengthCounts[i])
			length = float(i)
			lengthMeanDifferenceSquare = (length - mean) ** 2.0
			lengthMeanDifferenceSquareTotal = lengthCount * lengthMeanDifferenceSquare
			lengthSquaresSum += lengthMeanDifferenceSquareTotal

			if lengthSquaresSum < 0.0:
				print "Error: Float overflow in lengthSquaresSum (use fewer reads)"
				sys.exit(1)

	lengthSquaresSum /= float(readCount)
	stddev = math.sqrt(float(lengthSquaresSum))

	if roundDigits > 0:
		mean = round(mean, roundDigits)
		stddev = round(stddev, roundDigits)
	else:
		mean = int(mean + 0.5)
		stddev = int(stddev + 0.5)

	print str(mean) + "\t" + str(stddev)


def getline(data):
	line = data.readline()

	# Skip any headers
	while len(line) > 0 and line[0] == '@':
		line = data.readline()

	return line


def main():
	usage = """%prog -i <sam|stdin> [-n <max-reads>]
	
samStats v1.0 (build 793; Wed Sep 14 19:42:53 EDT 2011)
Matthew Titmus (mtitmus@cshl.edu)
SAM-file fragment size average/standard deviation calculator"""

	parser = OptionParser(usage)
	parser.add_option("-i", "--file", dest="inFile",
		help="A SAM-formatted file, or standard input (-i stdin).",
		metavar="FILE")
	parser.add_option("-n", "--maxreads", dest="maxReads", default=5000,
		help='Consider only the first N entries. 0 = all lines.',
		metavar="INT")
	parser.add_option("-m", "--maxlength", dest="maxLength", default=10000,
		help='Ignore reads with template lengths above this value. 0=No limit. Default=10000.',
		metavar="INT")
	parser.add_option("-r", "--round", dest="roundDigits", default=2,
		help='Round output to at most N digits. Rounding to 0 gives an integer results. Default=2.', metavar="INT")

	(opts, args) = parser.parse_args()

	if opts.inFile is None:
		parser.print_help()
		print
	else:
		doCounts(opts.inFile, int(opts.maxReads), maxLength=int(opts.maxLength), roundDigits=int(opts.roundDigits))


if __name__ == "__main__":
	sys.exit(main())
