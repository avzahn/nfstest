from time import time,sleep
from multiprocessing import Process
import numpy as np
from os import remove
from os.path import join
import sys
import h5py
import psutil

def gen_array(MB):
	N = int(MB*1e6/8.)
	return np.random.uniform(0,1,N)

def sleep_wait(Dt):
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


class fs_test(object):
	"""
	Test filesystem writes by writing a file of a given length
	repeatedly.

	Timing is done by naively checking the wall time before and
	after a write. This will tend to overestimate the write time,
	since we have no idea when the kernel will get around
	to us. On a non realtime system, I don't know if there's a 
	rigorous way to do this that doesn't involve kernel
	modifications, so we're just going to have to deal with
	only having upper bounds here.

	This also isn't the lowest overhead implementation, but
	we're only interested in testing at a few tens of MB/s.
	"""
	def __init__(self,chunksize,duration,fs,idn=0,rate=None,result_dir=''):

		self.rate = rate # MB/s
		self.chunksize = chunksize # MB
		self.duration = duration # seconds

		self.idn = idn

		self.payload = gen_array(chunksize)

		self.target = "%s_%s_%s.npy"%(rate,chunksize,idn)
		self.target = join(fs,self.target)

		self.result_dir = result_dir

		self.start_times = []
		self.finish_times = []

		self.period = chunksize/float(rate)

		msg = 'initialized fs_test %i: %i MB writes at %i MB/s to %s'%(idn,chunksize,rate,self.target)


		print msg
		sys.stdout.flush()


	def start(self):

		if self.rate == None:
			self.rate_unlimited_test()
		else:
			self.rate_limited_test()

	def remote_start(self):

		self.start()
		self.save()

	def save(self):

		fname = "%s_%s_%s.npy"%(self.rate,self.chunksize,self.idn)
		fname = join(self.result_dir,fname)
		np.save(fname,np.array( (self.start_times,self.finish_times) ) )




	def rate_unlimited_test(self):

		t0 = time()
		elapsed = 0.0

		while elapsed < self.duration:
			start = time()
			try:
				np.save(self.target,self.payload)
				finish = time()
				remove(self.target)
			except:
				finish = np.nan

			elapsed = time() - t0

			self.start_times.append(start)
			self.finish_times.append(finish)

	def rate_limited_test(self):

		t0 = time()
		elapsed = 0.0

		while elapsed < self.duration:
			start = time()

			try:
				np.save(self.target,self.payload)
				finish = time()
				remove(self.target)
			except:
				finish = np.nan

			dt = finish-start
			if dt < self.period:
				sleep_wait(self.period-dt)

			elapsed = time() - t0

			self.start_times.append(start)
			self.finish_times.append(finish)

class fs_multi_test(object):
	"""
	Manage multiple fs_test objects at once
	"""

	def __init__(self,rates,chunks,duration,fs,result_dir=''):

		self.result_dir = result_dir

		if isinstance(fs,str):
			fs = (fs for i in range(len(rates)))

		self.testers=[]
		for i,r,c,f in zip(range(len(rates)),rates,chunks,fs):
			self.testers.append(fs_test(rate=r,
				chunksize=c,
				idn=i,
				duration=duration,
				fs=f,
				result_dir=result_dir))

	def run(self):

		self.processes = [Process(target=test.remote_start) for test in self.testers]
		[p.start() for p in self.processes]


		t0 = time()
		elapsed = 0.0

		ram,swap,t = [],[],[]

		while elapsed < self.duration:

			ram.append(psutil.virtual_memory().used / 1e9)
			swap.append(psutil.swap_memory.used / 1e9)
			t.append(time())
			sleep_wait(1)
			elapsed = t[-1] - t0


		[p.join() for p in self.processes]

		mem_usage = np.array([ram,swap,t])
		np.save(join(self.result_dir,'memory_usage.npy'),mem_usage)
