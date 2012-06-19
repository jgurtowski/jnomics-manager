#!/usr/bin/env python

import sys
sys.path.append('./gen-py')

from jnomics_kbase_api import JnomicsCompute
from jnomics_kbase_api.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

transport = TSocket.TSocket("mshadoop1",7058)
transport.open()

protocol = TBinaryProtocol.TBinaryProtocol(transport)

service = JnomicsCompute.Client(protocol)

auth = Authentication("gurtowsk","james")


#print service.alignBowtie("reads.pe","motley","reads_aligned", auth)
#print service.mergeVCF("reads_gatk/variants_one", "reads_gatk/realign", "reads_gatk/variants_one.vcf", auth)

#print service.gatkCountCovariates("reads_gatk/realign","maize","hdfs://mshadoop1:8020/user/gurtowsk/reads_gatk/variants_one.vcf","reads_gatk/count_covar",auth)
#print service.mergeCovariate("reads_gatk/count_covar", "reads_gatk/covariates.cov",auth)
print service.gatkRecalibrate("reads_gatk/realign", "maize","hdfs://mshadoop1/user/gurtowsk/reads_gatk/covariates.cov","reads_gatk/recalibrate",auth)
