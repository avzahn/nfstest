from fs import *

rates = [20,1]
chunks=[20,1]

fs = '/home/gcp/data/nfstest'

test = fs_multi_test(rates,chunks,6,fs)

test.run()
