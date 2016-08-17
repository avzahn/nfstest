from fs import *

rates = [50,1,1,1]
chunks=[50,1,1,1]

fs = '/home/gcp/data/nfstest'

test = fs_multi_test(rates,chunks,6*3600,fs)

test.run()
