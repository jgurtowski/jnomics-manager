#!/usr/bin/env python

import sys
sys.path.append('./gen-py')

from jnomics_kbase_api import JnomicsCompute
from jnomics_kbase_api.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

transport = TSocket.TSocket("localhost",12343)
transport.open()

protocol = TBinaryProtocol.TBinaryProtocol(transport)

service = JnomicsCompute.Client(protocol)

auth = Authentication("james","james")


print service.alignBowtie("reads.pe","motley","reads_aligned", auth)


