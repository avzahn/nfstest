from fs import *

rates = [20,1]
chunks=[20,1]

fs = '/home/gcp/data/nfstest'

test = fs_multi_test(rates,chunks,60,'results3.h5',fs)

test.run()
