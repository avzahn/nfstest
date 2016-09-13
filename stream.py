from multiprocessing import Process, Pipe
from threading import Thread
from time import time, sleep
import numpy as np

def gen_array(MB):
	N = int(MB*1e6/8.)
	return np.random.uniform(0,1,N)

def wait(Dt):
	"""
	wait for Dt seconds, accurate to about ~10 ms
	"""
	t0 = time()
	wait = Dt/1000.
	elapsed = 0.0
	while elapsed < Dt:
		sleep(wait)
		elapsed = time() - t0
	return elapsed

def 

class bolostream(object):

	def __init__(self, targetfs, payload_size, write_period, conn, duration):

		self.write_period = write_period
		self.targetfs = targetfs

		self.conn = conn

		self.io = Thread(target=self.io_run)
		self.buff = []

		self.payload = np.random.uniform(0,1,payload_size)

		self.duration = duration

	def run(self):

		self.io.start()
		t0 = time()
		elapsed = 0

		while elapsed < self.duation:

			self.buff.append(np.copy(self.payload))
			wait(self.write_period)

			elapsed = time() - t0 

		self.io.join()

	def io_run(self):

		for arr in self.buff:

			





