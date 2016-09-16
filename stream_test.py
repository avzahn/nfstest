from stream import *

b = bolostream(targetfs='.',write_period=1,payload_size=16,duration=10)
g = gcpstream(targetfs='.',write_period=1,payload_size=16,duration=10)

m = stream_manager()
m.add(b,'b')
m.add(g,'g')

m.run('test.h5')
