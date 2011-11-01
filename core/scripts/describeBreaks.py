#!/usr/bin/env python

############################################################################
## 
##  Structural variation breakpoint identifier.
##  Matthew A. Titmus (mtitmus@cshl.edu), Cold Spring Harbor Labs, 2011 
##
##  Given a list of BEDPE-formatted putative breakpoints (specifically the 
##  sample.breaks.final file produced by the Hydra structural variation caller
##  (http://code.google.com/p/hydra-sv), this script attempts to cluster 
##  related breakpoints and if possible identify the particular type of 
##  variation (deletion, inversion, etc.) and determine position, size, 
##  and orientation relative to the reference. Furthermore, if the variation 
##  is a duplication or translocation, this script will also attempt to 
##  determine the origin of the duplicated or translocated sequence.
##
##  Revision: $Revision$
##  Date: $Date$
##
############################################################################

import sys
import getopt
import string
from optparse import OptionParser
from compiler.ast import For
import math

'''
Creation date: Jul 24, 2011
Revision number: $Revision$ 
Last edit date: $Date$

@author: Matthew Titmus
'''

__author__ = "Matthew Titmus"
__copyright__ = "Copyright 2011"
__credits__ = ["Matthew Titmus"]
__license__ = "GPL"
__version__ = "0.1-r$Revision$"
__email__ = "mtitmus@cshl.edu"
__status__ = "Development"

__DEBUG = True


DELETION = 'Deletion'
INVERSION = 'Inversion'
TRANS_INTER = 'Translocation (inter-chromosomal)'
TRANS_INTER_INVERTED = 'Translocation (inter-chromosomal; inverted)'
TRANS_INTRA = 'Translocation (intra-chromosomal)'
TRANS_INTRA_INVERTED = 'Translocation (intra-chromosomal; inverted)'
DUPLICATION_TANDEM = "Duplication (tandem)"
DUPLICATION_TANDEM_INVERTED = "Duplication (tandem; inverted)"
DUPLICATION_NONTANDEM = "Duplication (non-tandem)"
DUPLICATION_NONTANDEM_INVERTED = "Duplication (non-tandem; inverted)"
UNKNOWN	 = '[Unknown]'


class DescribeBreaks:
	def __init__(self, filter=0):
		self.breakPairs = []
		self.variants = [ ]
		self.fragmentSizeSD = 0.0
		self.filter = filter

		self.errorCount = 0
		self.deletionCount = 0
		self.duplicationNonTandemCount = 0
		self.duplicationNonTandemInvertedCount = 0
		self.duplicationTandemCount = 0
		self.duplicationTandemInvertedCount = 0
		self.inversionCount = 0
		self.translocationInterCount = 0
		self.translocationIntraCount = 0
		self.translocationIntraInvertedCount = 0
		self.translocationInterInvertedCount = 0


	def doDeletion(self, cluster):
		if (cluster[0].left.chr == cluster[0].right.chr):
			bp = cluster[0].clone()
			bp.type = DELETION
			bp.left.start = min(bp.left.end, bp.right.start)
			bp.right.end = max(bp.left.end, bp.right.start)
			bp.size = bp.right.end - bp.left.start
			self.deletionCount = self.deletionCount + 1

			self.variants += [bp]
			return True
		else:
			return False


	def doDuplicationNonTandem(self, cluster):
		if cluster[0].left.strand == '+':
			(plusminus, minusplus) = (cluster[0], cluster[1])
		else:
			(plusminus, minusplus) = (cluster[1], cluster[0])

		if plusminus.left.chr != minusplus.left.chr or plusminus.right.chr != minusplus.right.chr:
			return False

		overlapFraction = 0.05  # An overlap threshold of 5% filters trivial overlaps
		slop = int(round(self.fragmentSizeSD, 0))
		leftOverlap = plusminus.left.overlaps(minusplus.left, slop=slop, overlapFraction=overlapFraction)
		rightOverlap = plusminus.right.overlaps(minusplus.right, slop=slop, overlapFraction=overlapFraction)

		# If both ranges overlap, gradually increase the overlap fraction until we shake one out.
		while leftOverlap and rightOverlap and overlapFraction < 1.0:
			overlapFraction += 0.05
			leftOverlap = plusminus.left.overlaps(minusplus.left, slop=slop, overlapFraction=overlapFraction)
			rightOverlap = plusminus.right.overlaps(minusplus.right, slop=slop, overlapFraction=overlapFraction)

		# If both ranges still overlap to a significant degree, then kick this back because we don't know what it is.
		if leftOverlap == rightOverlap:
			return False

		# Signature AC BC: upstream duplication (duplicate upstream of duplicated)  
		elif rightOverlap: #F/T
			bp = plusminus.clone()
			bp.source = minusplus.right.clone()
			bp.source.start = min(plusminus.left.start, minusplus.left.end)
			bp.source.end = max(plusminus.left.start, minusplus.left.end)
			bp.left.start = min(plusminus.right.start, minusplus.right.end)
			bp.right.start = max(plusminus.right.start, minusplus.right.end)

		# Signature AC BC: downstream duplication (duplicate downstream of duplicated)
		elif leftOverlap: #T/F
			bp = plusminus.clone()
			bp.source = bp.right.clone()
			bp.source.start = min(plusminus.right.start, minusplus.right.end)
			bp.source.end = max(plusminus.right.start, minusplus.right.end)
			bp.left.start = min(plusminus.left.end, minusplus.left.start)
			bp.right.start = max(plusminus.left.end, minusplus.left.start)

		bp.type = DUPLICATION_NONTANDEM
		bp.id = plusminus.id + ',' + minusplus.id
		bp.numDistinctPairs = plusminus.numDistinctPairs + minusplus.numDistinctPairs

		bp.left.end = bp.left.start
		bp.right.end = bp.right.start
		bp.size = bp.source.end - bp.source.start + 1
		self.duplicationNonTandemCount = self.duplicationNonTandemCount + 1
		self.variants += [bp]
		return True


	def doDuplicationNonTandemInverted(self, cluster):
		if cluster[0].left.strand == '+':
			(plusplus, minusminus) = (cluster[0], cluster[1])
		else:
			(plusplus, minusminus) = (cluster[1], cluster[0])

		overlapFraction = 0.05  # An overlap threshold of 5% filters trivial overlaps
		slop = int(round(self.fragmentSizeSD, 0))
		leftOverlap = plusplus.left.overlaps(minusminus.left, slop=slop, overlapFraction=overlapFraction)
		rightOverlap = plusplus.right.overlaps(minusminus.right, slop=slop, overlapFraction=overlapFraction)

		# If both ranges overlap, gradually increase the overlap fraction until we shake one out.
		while leftOverlap and rightOverlap and overlapFraction < 1.0:
			overlapFraction += 0.05
			leftOverlap = plusplus.left.overlaps(minusminus.left, slop=slop, overlapFraction=overlapFraction)
			rightOverlap = plusplus.right.overlaps(minusminus.right, slop=slop, overlapFraction=overlapFraction)

		# If both ranges still overlap to a significant degree, then kick this back because we don't know what it is.
		if leftOverlap == rightOverlap:
			return False

		bp = plusplus.clone()
		bp.type = DUPLICATION_NONTANDEM_INVERTED
		bp.id = plusplus.id + ',' + minusminus.id
		bp.numDistinctPairs = plusplus.numDistinctPairs + minusminus.numDistinctPairs
		bp.source = bp.left.clone()

		# Signature AC BC: upstream duplication (duplicate upstream of duplicated)  
		if rightOverlap: #F/T
			bp.source.start = min(minusminus.left.start, plusplus.left.end)
			bp.source.end = max(minusminus.left.start, plusplus.left.end)
			bp.left.start = min(plusplus.right.end, minusminus.right.start)
			bp.right.start = max(plusplus.right.end, minusminus.right.start)

		# Signature AC BC: downstream duplication (duplicate downstream of duplicated)
		elif leftOverlap: #T/F
			bp.source.start = min(minusminus.right.start, plusplus.right.end)
			bp.source.end = max(minusminus.right.start, plusplus.right.end)
			bp.left.start = max(plusplus.left.start, minusminus.left.start)
			bp.right.start = min(plusplus.left.end, minusminus.left.end)

		bp.left.strand = '+'
		bp.source.strand = '-'
		bp.left.end = bp.left.start
		bp.right.end = bp.right.start
		bp.size = bp.source.end - bp.source.start + 1
		self.duplicationNonTandemInvertedCount += 1
		self.variants += [bp]

		return True


	def printRawCluster(self, cluster):
		for c in cluster:
			print ">" + str(c)


	def doDuplicationTandem(self, cluster):
		if cluster[0].left.chr != cluster[0].right.chr:
			return self.doTranslocationInterInverted(cluster)

		bp = cluster[0].clone()
		bp.type = DUPLICATION_TANDEM

		bp.source = bp.right.clone()
		bp.source.start = bp.left.start
		bp.source.end = bp.right.end
		bp.size = int(bp.size)

		start = bp.right.end - bp.size
		end = bp.left.start
		bp.left.start = start
		bp.left.end = start
		bp.right.start = end
		bp.right.end = end

		self.duplicationTandemCount += 1
		self.variants += [bp]
		return True


	def doDuplicationTandemInverted(self, cluster):
		bp = cluster[0].clone()
		bp.type = DUPLICATION_TANDEM_INVERTED

		bp.source = bp.right.clone()
		bp.source.start = bp.left.start
		bp.source.end = bp.right.end
		bp.size = int(bp.size)

		start = bp.right.end - bp.size
		end = bp.left.start
		bp.left.start = start
		bp.left.end = start
		bp.right.start = end
		bp.right.end = end

		bp.left.strand = '+'
		bp.source.strand = '-'
		self.duplicationTandemInvertedCount += 1
		self.variants += [bp]
		return True


	def doInversion(self, cluster):
		if cluster[0].left.chr != cluster[0].right.chr:
			return self.doDuplicationNonTandemInverted(cluster)

		if (cluster[0].left.strand == '+'):
			(plus, minus) = (cluster[0], cluster[1])
		else:
			(plus, minus) = (cluster[1], cluster[0])

		bp = minus.clone()
		minus = minus.clone()
		plus = plus.clone()

		bp.type = INVERSION
		bp.left.strand = '-'
		bp.left.start = max(minus.left.start, plus.left.start)
		bp.left.end = min(minus.left.end, plus.left.end)
		bp.right.start = max(minus.right.start, plus.right.start)
		bp.right.end = min(minus.right.end, plus.right.end)

		# The outer size of the element is end - start + 1, so the element's 
		# actual size is end - start - 1 (two bases smaller)
		start = float(bp.left.start + bp.left.end) / 2.0
		end = float(bp.right.start + bp.right.end) / 2.0
		bp.size = int(round(end - start - 1.0, 0))

		self.inversionCount += 1
		self.variants += [bp]
		return True


	def doTranslocationInter(self, cluster):
		if len(cluster) == 1:
			if cluster[0].left.strand == cluster[0].right.strand:
				return self.doTranslocationInterInverted(cluster)

			bp = cluster[0].clone()
			bp.size = 0
			bp.source = bp.right.clone()

			if bp.left.strand == '+':
				bp.left.start = bp.left.end
				bp.right.start = bp.left.end
				bp.right.end = bp.left.end
			else:
				bp.left.end = bp.left.start
				bp.right.start = bp.left.start
				bp.right.end = bp.left.start

			if bp.source.strand == '+':
				bp.source.start = bp.source.end
			else:
				bp.source.end = bp.source.start
		else:
			minusplus = None
			plusminus1 = None
			plusminus2 = None

			for bp in cluster:
				pattern = bp.left.strand + bp.right.strand
				bp.size = int(bp.size)

				if pattern == "-+":
					minusplus = bp
				elif plusminus1 is None:
					plusminus1 = bp
				else:
					plusminus2 = bp

			bp = plusminus1.clone()
			bp.id = minusplus.id + ',' + plusminus1.id + ',' + plusminus2.id
			bp.numDistinctPairs = minusplus.numDistinctPairs + plusminus1.numDistinctPairs + plusminus2.numDistinctPairs
			bp.source = bp.left.clone()

			# We can't tell for certain which breakpoint indicates the deletion 
			# (it could be either one, depending on whether the sequence was 
			# translocated upstream or downstream of its original position). We
			# assume therefore that the smaller one is the deletion. 

			# Element is translocated upstream  
			if plusminus1.size < plusminus2.size:
				bp.source.start = min(minusplus.left.start, plusminus1.left.end) + 1
				bp.source.end = max(plusminus1.right.start, plusminus2.left.end)
				bp.left.start = min(minusplus.right.end , plusminus2.right.start) + 1
				bp.right.start = max(minusplus.right.end , plusminus2.right.start)

			# Element is translocated downstream
			elif plusminus1.size >= plusminus2.size: #T/F
				bp.source.start = min(plusminus1.right.start, plusminus2.left.end) + 1
				bp.source.end = max(minusplus.right.end, plusminus2.right.start)
				bp.left.start = min(minusplus.left.start , plusminus1.left.end) + 1
				bp.right.start = max(minusplus.left.start , plusminus1.left.end)

			bp.left.end = bp.left.start
			bp.right.end = bp.right.start
			bp.size = bp.source.end - bp.source.start + 1

		# Somethign strange happened, and this probably doesn't actually fit
		# the signature we're looking for. Throw back to the caller.
		if bp.left.chr != bp.source.chr:
			return False

		bp.type = TRANS_INTER
		self.translocationInterCount += 1
		self.variants += [bp]
		return True


	def doTranslocation(self, cluster):
		if (cluster[0].left.chr != cluster[0].right.chr):
			return self.doTranslocationInter(cluster)

		minusplus = None
		plusminus1 = None
		plusminus2 = None

		cluster.sort() # Sort breakpoints by position
		for bp in cluster:
			pattern = bp.left.strand + bp.right.strand
			bp.size = int(bp.size)

			if pattern == "-+":
				minusplus = bp
			elif plusminus1 is None:
				plusminus1 = bp
			else:
				plusminus2 = bp

		bp = plusminus1.clone()
		bp.type = TRANS_INTRA
		bp.id = minusplus.id + ',' + plusminus1.id + ',' + plusminus2.id
		bp.numDistinctPairs = minusplus.numDistinctPairs + plusminus1.numDistinctPairs + plusminus2.numDistinctPairs
		bp.source = bp.left.clone()

		# We can't tell for certain which breakpoint indicates the deletion 
		# (it could be either one, depending on whether the sequence was 
		# translocated upstream or downstream of its original position. We
		# assume therefore that the smaller one is the deletion. 

		# Element is translocated upstream  
		if plusminus1.size < plusminus2.size:
			bp.source.start = min(minusplus.left.start, plusminus1.left.end) + 1
			bp.source.end = max(plusminus1.right.start, plusminus2.left.end)
			bp.left.start = min(minusplus.right.end , plusminus2.right.start) + 1
			bp.right.start = max(minusplus.right.end , plusminus2.right.start)

		# Element is translocated downstream
		elif plusminus1.size >= plusminus2.size: #T/F
			bp.source.start = min(plusminus1.right.start, plusminus2.left.end) + 1
			bp.source.end = max(minusplus.right.end, plusminus2.right.start)
			bp.left.start = min(minusplus.left.start , plusminus1.left.end) + 1
			bp.right.start = max(minusplus.left.start , plusminus1.left.end)

		bp.left.end = bp.left.start
		bp.right.end = bp.right.start
		bp.size = bp.source.end - bp.source.start + 1

		self.translocationIntraCount = self.translocationIntraCount + 1
		self.variants += [bp]
		return True


	def doTranslocationInterInverted(self, cluster):
		if len(cluster) == 1:
			bp = cluster[0].clone()
			bp.size = 0
			bp.source = bp.right.clone()

			if bp.left.strand == '+':
				bp.left.start = bp.left.end
				bp.right.start = bp.left.end
				bp.right.end = bp.left.end
			else:
				bp.left.end = bp.left.start
				bp.right.start = bp.left.start
				bp.right.end = bp.left.start

			if bp.source.strand == '+':
				bp.source.start = bp.source.end
			else:
				bp.source.end = bp.source.start
		else:
			plusminus = None
			minusminus = None
			plusplus = None

			cluster.sort() # Sort breakpoints by position
			for bp in cluster:
				pattern = bp.left.strand + bp.right.strand
				bp.size = int(bp.size)

				if pattern == "+-":
					plusminus = bp
				elif pattern == "--":
					minusminus = bp
				elif pattern == "++":
					plusplus = bp

			# When this is interchromosomal, we have to guess which is the interter
			# and which is the insertee. We do that by sizing the relative 
			# contributions of each chromosome

			chrLeftSize = max(plusminus.left.end, max(minusminus.left.end, plusplus.left.end)) - min(plusminus.left.start, min(plusplus.left.start, minusminus.left.start))
			chrRightSize = max(plusminus.right.end, max(minusminus.right.end, plusplus.right.end)) - min(plusminus.right.start, min(plusplus.right.start, minusminus.right.start))

			bp = plusminus.clone()
			bp.id = plusminus.id + ',' + plusplus.id + ',' + minusminus.id
			bp.numDistinctPairs = plusminus.numDistinctPairs + plusplus.numDistinctPairs + minusminus.numDistinctPairs

			# The larger one is the insert
			if chrRightSize > chrLeftSize:
				bp.source = bp.right.clone()
				bp.source.start = min(plusminus.right.start, min(plusplus.right.start, minusminus.right.start))
				bp.source.end = max(plusminus.right.end, max(minusminus.right.end, plusplus.right.end))
				bp.left.start = min(plusminus.left.start, min(plusplus.left.start, minusminus.left.start))
				bp.left.end = max(plusminus.left.end, max(minusminus.left.end, plusplus.left.end))
				bp.right.start = bp.left.start
				bp.right.end = bp.left.end
			else:
				bp.source = bp.left.clone()
				bp.source.start = min(plusminus.left.start, min(plusplus.left.start, minusminus.left.start))
				bp.source.end = max(plusminus.left.end, max(minusminus.left.end, plusplus.left.end))
				bp.left.start = min(plusminus.right.start, min(plusplus.right.start, minusminus.right.start))
				bp.left.end = max(plusminus.right.end, max(minusminus.right.end, plusplus.right.end))
				bp.right.start = bp.left.start
				bp.right.end = bp.left.end

		bp.left.strand = '+'
		bp.source.strand = '-'
		bp.size = bp.source.end - bp.source.start + 1
		bp.type = TRANS_INTER_INVERTED
		self.translocationInterInvertedCount += 1
		self.variants += [bp]

		return True


	def doTranslocationInverted(self, cluster):
		if (cluster[0].left.chr != cluster[0].right.chr):
			return self.doTranslocationInterInverted(cluster)

		plusminus = None
		minusminus = None
		plusplus = None

		cluster.sort() # Sort breakpoints by position
		for bp in cluster:
			pattern = bp.left.strand + bp.right.strand
			bp.size = int(bp.size)

			if pattern == "+-":
				plusminus = bp
			elif pattern == "--":
				minusminus = bp
			elif pattern == "++":
				plusplus = bp

		overlapFraction = 0.00
		slop = int(round(self.fragmentSizeSD, 0))
		upstreamOverlap = plusminus.left.overlaps(minusminus.left, slop=slop, overlapFraction=overlapFraction)
		downstreamOverlap = plusminus.right.overlaps(plusplus.right, slop=slop, overlapFraction=overlapFraction)

		# If both ranges overlap, gradually increase the overlap fraction until we shake one out.
		while upstreamOverlap and downstreamOverlap and overlapFraction < 1.0:
			overlapFraction += 0.05
			upstreamOverlap = plusplus.left.overlaps(minusminus.left, slop=slop, overlapFraction=overlapFraction)
			downstreamOverlap = plusplus.right.overlaps(minusminus.right, slop=slop, overlapFraction=overlapFraction)

		# If both ranges still overlap to a significant degree, then bail out
		if upstreamOverlap == downstreamOverlap:
			return False

		bp = plusplus.clone()
		bp.type = TRANS_INTRA_INVERTED
		bp.left.strand = '-'
		bp.id = plusminus.id + ',' + plusplus.id + ',' + minusminus.id
		bp.numDistinctPairs = plusminus.numDistinctPairs + plusplus.numDistinctPairs + minusminus.numDistinctPairs
		bp.source = bp.left.clone()

		# Element is translocated upstream
		if upstreamOverlap and not downstreamOverlap:
			bp.source.start = min(plusminus.left.end, minusminus.left.start) + 1
			bp.source.end = max(plusminus.right.start, plusplus.left.end)
			bp.left.start = min(minusminus.right.start, plusplus.right.end) + 1
			bp.right.start = max(minusminus.right.start, plusplus.right.end)

		# Element is translocated downstream
		elif not upstreamOverlap and downstreamOverlap:
			bp.source.start = min(plusminus.left.end, minusminus.right.start) + 1
			bp.source.end = max(plusminus.right.start, plusplus.right.end)
			bp.left.start = min(plusplus.left.end, minusminus.left.start) + 1
			bp.right.start = max(plusplus.left.end, minusminus.left.start)
		else:
			return False

		bp.left.strand = '+'
		bp.source.strand = '-'
		bp.left.end = bp.left.start
		bp.right.end = bp.right.start
		bp.size = bp.source.end - bp.source.start + 1

		self.translocationIntraInvertedCount += 1
		self.variants += [bp]
		return True


	def printCluster(self, isResolved, index, cluster):
		columns = []
		text = ""
		pairsSum = 0

		# Count contributing breakpoints
		for c in cluster:
			pairsSum += int(c.numDistinctPairs)

		if self.filter is not None and int(pairsSum) < int(self.filter):
			return

		for j in range(0, 13):
			columns.append("")

		for j in range(0, len(cluster)):
			bp = cluster[j]
			uid = str(index) + '.' + str(j + 1)

			columns[0] = bp.left.chr
			columns[1] = bp.left.start
			columns[6] = str(index)
			columns[7] = bp.numDistinctPairs  # "score"
			columns[8] = bp.left.strand
			columns[10] = bp.type
			columns[12] = bp.id

			if len(cluster) > 1:
				columns[6] += "." + str(j + 1)

			if isResolved:
				columns[2] = bp.right.end

				if bp.source is None:
					columns[3] = "*"
					columns[4] = "*"
					columns[5] = "*"
					columns[9] = "*"
				else:
					columns[3] = bp.source.chr
					columns[4] = bp.source.start
					columns[5] = bp.source.end
					columns[9] = bp.source.strand

				 # Size 0 is used to indicate "unknown or not applicable"
				if int(bp.size) == 0:
					columns[11] = "*"
				else:
					columns[11] = bp.size

				if (cluster[j].source is None):
					assert cluster[j].left.chr == cluster[j].right.chr
			else:
				columns[2] = bp.left.end
				columns[3] = bp.right.chr
				columns[4] = bp.right.start
				columns[5] = bp.right.end
				columns[9] = bp.right.strand

				 # Size 0 is used to indicate "unknown or not application"
				if int(bp.size) < 1:
					columns[11] = "*"
				else:
					columns[11] = bp.size

			text = "" + columns[0]
			for i in range(1, len(columns)):
				text += "\t" + str(columns[i])

			print str(text)
#			assert cluster[j].size >= 0


	def evaluateBreakPairs(self, slop=0):
		unresolved = []
		unresolved += self.breakPairs
		overlapFraction = 0.20
		originalOverlapFraction = overlapFraction
		originalSlop = slop
		percentTweak = 0.05

		print "# sv-chr\tsv-start\tsv-end\torig-chr\torig-start\torig-end\tuid\tscore/#reads\tsv-orient\torig-orient\tdescription\tsize\thydra-bp-ids"

		while len(unresolved) > 0 and slop >= 0 and overlapFraction <= 1.0:
			counter = 0
			unresolved.sort()
			iterator = BreakpointClusterIterator(unresolved, slop=slop, overlapFraction=overlapFraction)
			unresolved = []

			while iterator.hasNext():
				clusterUnmerged = iterator.next()
				clusterMerged = self.mergeBreakPairs(clusterUnmerged, slop=slop, overlapFraction=overlapFraction)
				counter += 1

				# 0: Interchromosomal translocations
				if len(clusterMerged) == 1 and clusterMerged[0].left.chr != clusterMerged[0].right.chr:
					if self.doTranslocationInter(clusterMerged):
						self.printCluster(True, counter, self.variants)
						self.variants = []
						continue

				# 1: Deletion
				if self.matchStrands(clusterMerged, [ "+-" ]) and self.doDeletion(clusterMerged):
					self.printCluster(True, counter, self.variants)
					self.variants = []
					continue

				# 2-4: Inversion or inverted duplication (tandem or non-tandem)
				if self.matchStrands(clusterMerged, [ "++", "--" ]):
					if (clusterMerged[0].left.strand == '+'):
						(plus, minus) = (clusterMerged[0], clusterMerged[1])
					else:
						(plus, minus) = (clusterMerged[1], clusterMerged[0])

					# If this is an inversion, then both ends of each breakpoint will overlap
					if plus.overlaps(minus, observeChr=True, observeStrand=False, eitherEnd=False, slop=slop) and self.doInversion(clusterMerged):
						assert len(self.variants) > 0
						self.printCluster(True, counter, self.variants)
						self.variants = []
						continue

					# For an inverted tandem duplication, both ends of the plus-plus break should overlap
					if plus.left.overlaps(plus.right, observeChr=True, observeStrand=True, slop=slop) and self.doDuplicationTandemInverted(clusterMerged):
						assert len(self.variants) > 0
						self.printCluster(True, counter, self.variants)
						self.variants = []
						continue

					if self.doDuplicationNonTandemInverted(clusterMerged):
						assert len(self.variants) > 0
						self.printCluster(True, counter, self.variants)
						self.variants = []
						continue

				# 5: Tandem duplication
				if self.matchStrands(clusterMerged, [ "-+" ]) and self.doDuplicationTandem(clusterMerged):
					assert len(self.variants) > 0
					self.printCluster(True, counter, self.variants)
					self.variants = []
					continue

				# 6: Non-Tandem duplication
				if self.matchStrands(clusterMerged, [ "+-", "-+" ]) and self.doDuplicationNonTandem(clusterMerged):
					assert len(self.variants) > 0
					self.printCluster(True, counter, self.variants)
					self.variants = []
					continue

				# 7: Translocation (deletion + duplication)
				if self.matchStrands(clusterMerged, [ "+-", "-+", "+-" ]) and self.doTranslocation(clusterMerged):
					assert len(self.variants) > 0
					self.printCluster(True, counter, self.variants)
					self.variants = []
					continue

				# 8: Inverted translocation
				if self.matchStrands(clusterMerged, [ "++", "--", "+-" ]) and self.doTranslocationInverted(clusterMerged):
					assert len(self.variants) > 0
					self.printCluster(True, counter, self.variants)
					self.variants = []
					continue

				# Something else...
				if len(clusterMerged) <= 5:
					self.printCluster(False, counter, clusterMerged)
					self.variants = []
					continue

				counter -= 1
				unresolved += clusterUnmerged
			# End: while iterator.hasNext()

			# Slightly decrease the slop and increase the overlap fraction
			overlapFraction *= (1.00 + percentTweak)
			slop *= (1.00 - percentTweak)
		# End: while len(unresolved) > 0 and overlapFraction < 0.25

		iterator = BreakpointClusterIterator(unresolved, slop=slop, overlapFraction=overlapFraction)
		unresolved.sort()
		while iterator.hasNext():
			counter += 1
			self.printCluster(False, counter, unresolved)

		duplicationsTandem = self.duplicationTandemCount + self.duplicationTandemInvertedCount
		duplicationsNonTandem = self.duplicationNonTandemCount + self.duplicationNonTandemInvertedCount
		duplicationsTotal = duplicationsTandem + duplicationsNonTandem
		translocationsTotal = self.translocationInterCount + self.translocationIntraCount + self.translocationIntraInvertedCount + self.translocationInterInvertedCount

		print
		print "# Deletions: " + str(self.deletionCount)
		print "# Inversions: " + str(self.inversionCount)
		print "# Duplications: " + str(duplicationsTotal) + " total"
		print "#    Non-tandem: " + str(duplicationsNonTandem) + " (" + str(self.duplicationNonTandemInvertedCount) + " inverted)"
		print "#    Tandem: " + str(duplicationsTandem) + " (" + str(self.duplicationTandemInvertedCount) + " inverted)"
		print "# Translocations: " + str(self.translocationInterCount + self.translocationIntraCount) + " total"
		print "#    Intra-chromosomal: " + str(self.translocationIntraCount + self.translocationIntraInvertedCount) + " (" + str(self.translocationIntraInvertedCount) + " inverted)"
		print "#    Inter-chromosomal: " + str(self.translocationInterCount + self.translocationInterInvertedCount) + " (" + str(self.translocationInterInvertedCount) + " inverted)"
		print "# Unresolved breaks: " + str(len(unresolved))

		if self.errorCount > 0:
			print "# Errors: " + str(self.errorCount)

		print "# Total: " + str(len(self.variants))


	def loadBreakPairs(self, inFile):
		lineCount = 0
		splits = []

		if inFile == "stdin":
			data = sys.stdin
		else:
			data = open(inFile, 'r')

		sum = 0
		values = []

		for line in data:
			splits = line.rstrip().split('\t')

			bp = Breakpoint()
			bp.left.chr = splits[0]		 # "chrom1"
			bp.left.start = int(splits[1])  # "start1"
			bp.left.end = int(splits[2])	# "end1" 
			bp.left.strand = splits[8]	  # "strand1"
			bp.right.chr = splits[3]		# "chrom2"
			bp.right.start = int(splits[4]) # "start2"
			bp.right.end = int(splits[5])   # "end2"
			bp.right.strand = splits[9]	 # "strand2"
			bp.id = splits[6] # breakpointId
			bp.size = splits[14] # "breakpointSize"
			bp.numDistinctPairs = int(splits[7])	# "numDistinctPairs" 

			sum += bp.left.end - bp.left.start
			sum += bp.right.end - bp.right.start
			values += [bp.left.end - bp.left.start, bp.right.end - bp.right.start]

			self.breakPairs.append(bp)
			lineCount = lineCount + 1

		mean = float(sum) / float(len(values))
		mean = round(mean, 2)

		b = 0.0
		for v in values:
			b += (float(v) - mean) ** 2.0

		stddev = (b / float(len(values))) ** 0.5
		stddev = round(stddev, 2)

		self.fragmentSizeMean = round(mean, 2)
		self.fragmentSizeSD = round(stddev, 2)

		self.breakPairs.sort();
		print "# " + str(lineCount) + " paired breaks read (fragment size mean=" + str(mean) + ", SD=" + str(stddev) + ")"



	# cluster: a list of BreakPairs
	# orientations: A list of orientations as: [ "+-", ++" ]
	# Returns TRUE if all match; False otherwise
	def matchStrands(self, cluster, orientations):
		clusterCopy = []
		clusterCopy += cluster

		for os in orientations:
			left = os[0]
			right = os[1]
			osResult = False

			i = 0
			while osResult == False and i < len(clusterCopy):
				breakPair = clusterCopy[i]

				if left == breakPair.left.strand and right == breakPair.right.strand:
					osResult = True
					clusterCopy.pop(i)
				else:
					i += 1

			if osResult == False:
				return False

		return 0 == len(clusterCopy)


	#	overlapFraction: Minimum overlap required as fraction of each range
	#			(1.0 = 100%). Default=1E-9 (effectively 1bp) 
	#	slop: The amount of overlap "slop", in b.p. (added directly to the overlap calculation)
	def mergeBreakPairs(self, cluster=None, overlapFraction=1E-9, slop=0):
		if (cluster == None):
			breakPairsList = self.breakPairs
		else:
			breakPairsList = cluster

		if (len(breakPairsList) < 1):
			return

		slop = int(0.5 + slop)
		mergeCount = 0
		readAhead = breakPairsList[0].clone()
		newBreakpairs = []

		for bp in breakPairsList[1:]:
			if readAhead.overlaps(bp, True, overlapFraction=overlapFraction, slop=slop):
				ids = bp.id.split(',')
				ids.extend(readAhead.id.split(','))
				ids.sort()

				newid = ids[0]
				for i in ids[1:]:
					newid = newid + ',' + i

				readAhead.left.start = min(readAhead.left.start, bp.left.start)
				readAhead.left.end = max(readAhead.left.end, bp.left.end)
				readAhead.right.start = min(readAhead.right.start, bp.right.start)
				readAhead.right.end = max(readAhead.right.end, bp.right.end)
				readAhead.contributors = readAhead.contributors + 1
				readAhead.id = newid
				readAhead.numDistinctPairs += bp.numDistinctPairs

				mergeCount = mergeCount + 1
			else:
				newBreakpairs.append(readAhead)
				readAhead = bp.clone();

		newBreakpairs.append(readAhead)

		return newBreakpairs



class Breakpoint(object):
	contributors = 1
	left = None
	right = None
	id = None
	size = 0
	type = UNKNOWN # Inversion, deletion, etc.
	source = None # A Position; defined for duplications and translocationsbp.
	clustered = False


	def __eq__(self, other):
		return self.left == other.left and self.right == other.right


	def __gt__(self, other):
		return self.left > other.left


	def __init__(self):
		self.contributors = 1
		self.left = Position()
		self.right = Position()
		self.id = None
		self.numDistinctPairs = 0
		self.size = 0
		self.type = UNKNOWN # Inversion, deletion, etc.
		self.source = None # A Position; defined for duplications and translocations
		self.clustered = False # Has this already been assigned to a cluster?


	def __lt__(self, other):
		return self.left < other.left


	def __ne__(self, other):
		return not self.__eq__(other)


	def __str__(self, *args, **kwargs):
		text = str(self.left.chr) + '\t' + str(self.left.start) + '\t' + str(self.left.end)
		text = text + '\t' + str(self.right.chr) + '\t' + str(self.right.start) + '\t' + str(self.right.end)
		text = text + '\t' + str(self.id)
		text = text + '\t' + str(self.numDistinctPairs)
		text = text + '\t' + str(self.left.strand) + '\t' + str(self.right.strand)

		if not self.source is None:
			text = text + '\t' + str(self.source)

		return text


	def clone(self):
		clone = Breakpoint()
		clone.type = self.type
		clone.left = self.left.clone()
		clone.right = self.right.clone()
		clone.id = self.id
		clone.numDistinctPairs = self.numDistinctPairs
		clone.size = self.size
		clone.contributors = self.contributors

		return clone


	# Returns True if and only if this and the breakpoint parameter shares the 
	# same chromosome and both end ranges overlap by at least one base. 
	# Params:
	#	observeStrand: if True, then strands must match as well.
	#	eitherEnd: if True, then this returns true if EITHER end of this
	#			breakpoint overlaps EITHER end of the breakPoint parameter.
	#	overlapFraction: Minimum overlap required as fraction of this Position 
	#			(1.0 = 100%). Default=1E-9 (effectively 1bp) 
	#	slop: The amount of slop (in b.p.) (added directly to the overlap calculation)
	def overlaps(self, breakPoint, observeChr=False, observeStrand=False, eitherEnd=False, overlapFraction=1E-9, slop=0):
		if breakPoint is None:
			result = False
		elif eitherEnd == False:
			result = self.left.overlaps(breakPoint.left, observeChr, observeStrand, overlapFraction=overlapFraction, slop=slop) and self.right.overlaps(breakPoint.right, observeChr, observeStrand, overlapFraction=overlapFraction, slop=slop)
		else:
			result = self.left.overlaps(breakPoint.left, observeChr, observeStrand, overlapFraction=overlapFraction, slop=slop) or self.left.overlaps(breakPoint.right, observeChr, observeStrand, overlapFraction=overlapFraction, slop=slop) or self.right.overlaps(breakPoint.left, observeStrand, overlapFraction=overlapFraction, slop=slop) or self.right.overlaps(breakPoint.right, observeStrand, overlapFraction=overlapFraction, slop=slop)

		return result



class BreakpointClusterIterator(object):
	_positionsLookup = []

	def __init__(self, breakPairs, slop=0, overlapFraction=1E-9):
		#self._breakpoints = []
		#self._breakpoints += breakPairs
		self.slop = slop
		self.overlapFraction = overlapFraction
		self._positionsLookup = []

		for bp in breakPairs:
			self._positionsLookup += [ PositionLookup(bp, bp.left), PositionLookup(bp, bp.right) ]

		self._positionsLookup.sort()


	# Returns the index in _positionsLookup of the first breakpoint it finds
	# that overlaps the given position between the indices of first and last
	# (inclusive); NOT NECESSARILY the first matching index in the list! 
	def _findAnyMatch(self, position, first=0, last= -1, slop=0):
		if (len(self._positionsLookup) == 0):
			return None
		elif (last == -1):
			last = len(self._positionsLookup) - 1

		mid = (first + last) / 2

		queryPosition = self._positionsLookup[mid].position
		overlapAmount = position.overlapAmount(queryPosition, slop=slop)

		# A non-positive overlap amount indicates a non-overlap
		if overlapAmount < 1:
			if first == last:
				return None
			elif position.start < queryPosition.start:
				return self._findAnyMatch(position, first, mid, slop=slop)
			else:
				return self._findAnyMatch(position, mid + 1, last, slop=slop)
		else:
			return mid


	def _findClusterFor(self, breakpoint, allowedChrs=None, recursive=False):
		if allowedChrs is None:
			allowedChrs = [ breakpoint.left.chr, breakpoint.right.chr ]

		cluster = self._findOverlapping(breakpoint.left, allowedChrs=allowedChrs) + self._findOverlapping(breakpoint.right, allowedChrs=allowedChrs)

		if recursive == True:
			r1Cluster = cluster

			while len(r1Cluster) > 0:
				r2Cluster = []
				for bp in r1Cluster:
					r2Cluster += self._findClusterFor(bp, allowedChrs=allowedChrs, recursive=False)

				cluster += r2Cluster
				r1Cluster = r2Cluster

		return cluster


	def _findOverlapping(self, position, allowedChrs=None):
		cluster = []
		i = self._findAnyMatch(position)

		if allowedChrs is None:
			allowedChrs = [ position.chr ]

		if i != None:
			# Since 'i' is only known to match anywhere in a range of matching
			# positions, an unknown number of positions both above and below
			# 'i' may match.

			# Seek up until we find a non-overlapping position
			upper = i
			while upper < len(self._positionsLookup):
				queryPosition = self._positionsLookup[upper].position
				queryBP = self._positionsLookup[upper].breakpoint

				# The ranges don't overlap; bail
				if position.overlapAmount(queryPosition, slop=self.slop) < 0:
					break;

				# If the position is on a different chromosome; ignore it.
				if not queryBP.left.chr in allowedChrs or not queryBP.right.chr in allowedChrs:
					upper += 1
					continue

				del self._positionsLookup[upper]

				# If the breakpoint has already been clustered, drop it.
				if not queryBP.clustered:
					queryBP.clustered = True
					cluster += [ queryBP ]

			# Now seek down until we find a non-overlapping position
			lower = i - 1
			while lower >= 0 and lower < len(self._positionsLookup):
				queryPosition = self._positionsLookup[lower].position
				queryBP = self._positionsLookup[lower].breakpoint

				# The ranges don't overlap; bail
				if position.overlapAmount(queryPosition, slop=self.slop) < 0:
					break;

				# If the position is on a different chromosome; ignore it.	
				if not queryBP.left.chr in allowedChrs or not queryBP.right.chr in allowedChrs:
					lower -= 1
					continue

				del self._positionsLookup[lower]

				# If the breakpoint has already been clustered, drop it.
				if not queryBP.clustered:
					queryBP.clustered = True
					cluster += [ queryBP ]

		return cluster


	def hasNext(self):
		while len(self._positionsLookup) > 0 and self._positionsLookup[0].breakpoint.clustered == True:
			del self._positionsLookup[0]

		return len(self._positionsLookup) > 0


	def next(self):
		assert self.hasNext()

		head = self._positionsLookup.pop(0)
		head.breakpoint.clustered = True

		cluster = [head.breakpoint] + self._findClusterFor(head.breakpoint, recursive=True)
		cluster.sort()

		return cluster



class Position(object):
	chr = None
	strand = None
	start = 0
	end = 0

	def __init__(self, start=0, end=0, chr=None, strand=None):
		self.start = start
		self.end = end
		self.chr = chr
		self.strand = strand


	def __eq__(self, other):
		return self.start == other.start and self.end == other.end and self.strand == other.strand and self.chr == other.chr


	def __lt__(self, other):
		selfIsNumber = isNumber(self.chr)
		otherIsNumber = isNumber(other.chr)

		if selfIsNumber == otherIsNumber:
			return self.chr < other.chr
		else:
			return selfIsNumber


	def __gt__(self, other):
		selfIsNumber = isNumber(self.chr)
		otherIsNumber = isNumber(other.chr)

		if selfIsNumber == otherIsNumber:
			return self.chr > other.chr
		else:
			return otherIsNumber


	def __ne__(self, other):
		return not self.__eq__(other)


	def __str__(self, *args, **kwargs):
		if self.start != self.end:
			pos = str(self.start) + '\t' + str(self.end)
		else:
			pos = str(self.end) + '\t'

		return str(self.chr) + '\t' + pos + '\t' + str(self.strand)


	def clone(self):
		clone = Position()
		clone.chr = self.chr
		clone.strand = self.strand
		clone.start = self.start
		clone.end = self.end
		return clone


	def overlapAmount(self, position, slop=0):
		if (self.start <= position.start):
			p1 = self
			p2 = position
		else:
			p1 = position
			p2 = self

		return slop + (p1.end - p2.start) + 1


	# Returns True if and only if this and the breakpoint parameter share the 
	# same chromosome and have overlapping (share at least one base) ranges.
	# 
	# Params:
	#	observeStrand: If True, then the strands must match.
	#	overlapFraction: Minimum overlap required as fraction of this Position 
	#			(1.0 = 100%). Default=1E-9 (effectively 1bp) 
	#	slop: The amount of slop (in b.p.) (added directly to the overlap calculation)
	def overlaps(self, position, observeChr=False, observeStrand=False, overlapFraction=1E-9, slop=0):
		assert position is not None

		overlap = self.overlapAmount(position, slop)

		if overlap < 0:
			return False
		else:
			minOverlap = float(self.end - self.start) * overlapFraction

			return (overlap > minOverlap) and ((not observeChr) or (self.chr == position.chr)) and ((not observeStrand) or (self.strand == position.strand))



class PositionLookup:
	breakpoint = None
	position = None

	def __init__(self, breakpoint, position):
		self.breakpoint = breakpoint
		self.position = position

	def __lt__(self, other):
		return self.position.start < other.position.start



class Tests(object):
	def runTests(self):
		self.testPositionOverlapAmount()
		self.testPositionOverlaps()
		self.testBreakpointClusterIterator()
		print "All tests successful"


	def testBreakpointClusterIterator(self):
		bp1_1 = Breakpoint()
		bp1_1.left = Position(start=100, end=500)
		bp1_1.right = Position(start=1000, end=1500)

		bp1_2 = Breakpoint()
		bp1_2.left = Position(start=5, end=505)
		bp1_2.right = Position(start=1005, end=1505)

		bp2_1 = Breakpoint()
		bp2_1.left = Position(start=2000, end=2500)
		bp2_1.right = Position(start=2100, end=2700)

		iter = BreakpointClusterIterator([bp1_1, bp1_2, bp2_1], slop=0)

		# Are the positions sorted correctly? 
		assert iter._positionsLookup[0].position.start == 5
		assert iter._positionsLookup[1].position.start == 100
		assert iter._positionsLookup[2].position.start == 1000
		assert iter._positionsLookup[3].position.start == 1005
		assert iter._positionsLookup[4].position.start == 2000
		assert iter._positionsLookup[5].position.start == 2100

		for p in iter._positionsLookup:
			print str(p.position)

		assert 0 == iter._findAnyMatch(Position(start=1, end=50))
		assert 5 == iter._findAnyMatch(Position(start=2600, end=2750))

		iter = BreakpointClusterIterator([bp1_1, bp1_2, bp2_1], slop=0)

		# We should have two iterations
		assert iter.hasNext()
		assert len(iter.next()) == 2
		assert iter.hasNext()
		assert len(iter.next()) == 1
		assert not iter.hasNext()


	def testPositionOverlaps(self):
		p1 = Position(start=1, end=100, chr="X", strand="+")
		p2 = Position(start=1, end=100, chr="Y", strand="-")
		assert p1.overlaps(p2, observeChr=False, observeStrand=False, slop=0)
		assert not p1.overlaps(p2, observeChr=False, observeStrand=True, slop=0)
		assert not p1.overlaps(p2, observeChr=True, observeStrand=False, slop=0)
		assert not p1.overlaps(p2, observeChr=True, observeStrand=True, slop=0)

		p1 = Position(start=1, end=100)
		p2 = Position(start=100, end=1000)
		assert p1.overlaps(p2, slop=0)

		p1 = Position(start=1, end=100)
		p2 = Position(start=101, end=1000)
		assert p1.overlapAmount(p2, slop=1)
		assert not p1.overlapAmount(p2, slop=0)


	def testPositionOverlapAmount(self):
		p1 = Position(start=1, end=100)
		p2 = Position(start=1, end=100)
		assert p1.overlapAmount(p2, slop=0) == 100
		assert p2.overlapAmount(p1, slop=0) == 100

		# This method should not care about chromosome or strand
		p1 = Position(start=1, end=100, chr="X", strand="+")
		p2 = Position(start=1, end=100, chr="Y", strand="-")
		assert p1.overlapAmount(p2, slop=0) == 100
		assert p2.overlapAmount(p1, slop=0) == 100

		p1 = Position(start=1, end=100)
		p2 = Position(start=100, end=1000)
		assert p1.overlapAmount(p2, slop=0) == 1

		p2 = Position(start=101, end=1000)
		assert p1.overlapAmount(p2, slop=0) == 0
		assert p1.overlapAmount(p2, slop=1) == 1

		# non-overlaps should give negative values in both directions
		p1 = Position(start=1000, end=2000)
		p2 = Position(start=3000, end=4000)
		assert p1.overlapAmount(p2, slop=0) < 0
		assert p1.overlapAmount(p2, slop=0) < 0




def debug(str):
	if __DEBUG:
		print "[DEBUG] " + str


def main():
	usage = """%prog -i <sample.breaks.final> 
	
describeBreaks v1.0 (build $Revision; $Date$)
Matthew Titmus (mtitmus@cshl.edu)
Attempts to identify structural variations from breakpoints listed in
sample.breaks.final files produced by the Hydra structural variation caller
(http://code.google.com/p/hydra-sv).

This script attempts to cluster related breakpoints and identify the types of 
the underlying structural variations (deletion, inversion, etc.) and determine
their positions, sizes, and orientations relative to the reference. Furthermore,
if a variation is a duplication or translocation, this script will also attempt
to determine the origin of the duplicated or translocated sequence. Breakpoints
that appear to be related but whose underlying variation cannot be determined 
are listed as unidentified clusters."""

	parser = OptionParser(usage)

	parser.add_option("-i", "--file", dest="inFile",
		help="A tab-delimited hydra.breakPairs.final file, or standard input (-i stdin).",
		metavar="FILE")

	parser.add_option("-f", "--filter", dest="filter",
		help="Filter clusters with fewer than this number of contributing reads.",
		metavar="INT")

	(opts, args) = parser.parse_args()

	if opts.inFile is None:
		parser.print_help()
		print
	else:
		describeBreaks = DescribeBreaks(filter=opts.filter)
		describeBreaks.loadBreakPairs(opts.inFile)
		describeBreaks.evaluateBreakPairs(slop=int(describeBreaks.fragmentSizeSD * 2))


def isNumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def max(val1, val2):
	if val1 > val2:
		return val1
	else:
		return val2


def min(val1, val2):
	if val1 < val2:
		return val1
	else:
		return val2


def _runTests():
	Tests().runTests();


if __name__ == "__main__":
	sys.exit(main())
#	sys.exit(_runTests())






class Variation:
	id = None
	type = None

	def __init__(self):
		pass



class ElementaryVariation(Variation):
	DELETION = 'Deletion'
	INVERSION = 'Inversion'
	INSERTION = 'Insertion'
	UNKNOWN	 = '[Unknown]'

	position = None
	strand = '.'

	size = 0
	type = UNKNOWN # Inversion, deletion, etc.
	source = None # A Position; defined for duplications and translocations

	def __init__(self):
		self.variationType = ElementaryVariation.UNKNOWN
		self.position = Position()



#class CompoundVariation(Variation):
#	TRANS_INTER = 'Translocation (inter-chromosomal)'
#	TRANS_INTRA = 'Translocation (intra-chromosomal)'
#	TRANS_INTER_INVERTED = 'Translocation (inter-chromosomal; inverted)'
#	TRANS_INTRA_INVERTED = 'Translocation (intra-chromosomal; inverted)'
#	DUPLICATION_TANDEM = "Duplication (tandem)"
#	DUPLICATION_TANDEM_INVERTED = "Duplication (tandem; inverted)"
#	DUPLICATION_NONTANDEM = "Duplication (non-tandem)"
#	DUPLICATION_NONTANDEM_INVERTED = "Duplication (non-tandem; inverted)"
#	UNKNOWN	 = '[Unknown]'
