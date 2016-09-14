from multiprocessing import Process, Pipe
from threading import Thread
from time import time, sleep
from os import remove
from os.path import join
import random
import numpy as np
import h5py
import psutil

def wait(Dt):
	"""
	sleep wait for Dt seconds, accurate to about ~10 ms
	"""
	t0 = time()
	wait = Dt/1000.
	elapsed = 0.0
	while elapsed < Dt:
		sleep(wait)
		elapsed = time() - t0
	return elapsed

def strcopy(s):
	"""
	Bypass python string interning
	"""
	return '%s'%s

def gen_payload(nbytes):

	payload = ''
	for i in range(nbytes):
		c = chr(random.randint(0,127))
		payload += c

	return payload


class stream(object):
	"""
	Parent class for simulating parallel file system write loads with random
	data.

	Each subclass must implement a run() method which saves results to self.results
	and then sends it through the pipe connection self.conn by calling self.report()

	Preferably, self.results should be an n by 3 numpy array of 
	[start_time, stop_time, bytes_written] for each simulated write
	"""

	def __init__(self, targetfs, payload_size, write_period, duration):
		"""
		Each stream will attempt to write payload_size bytes every write_period
		seconds to a unique file in directory targetfs for duration seconds.
		"""
		
		self.write_period = write_period
		self.targetfs = targetfs

		self.fname = '%s_%s_%i'%(payload_size,write_period, id(self))
		self.fname = join(targetfs,self.fname)

		self.parent_conn = None
		self.process = None
		self.conn = None

		self.payload = gen_payload(payload_size)

		self.duration = duration

		self.file = None

		self.results = []

	def report(self,results):
		"""
		We want a stream_manager to be able to call self.run
		inside a new Process, with which it can't share memory.
		"""

		self.results = results

		if self.conn:
			# if self.run was called by a stream_manager, it would
			# have assigned self.conn, and will expect to recv
			# self.results and save them to its process local version
			# of self's self.results. see stream_manager.run
			self.conn.send(results)

class stream_manager(object):
	"""
	Manage multiple streams running in parallel. Just self.add() existing
	stream objects and then self.run() followed by self.save(fname)
	"""

	def __init__(self):

		self.streamers = {}
		self.ram,self.swap,self.t = [],[],[]

		self.duration = 0

	def add(self, streamer, name):

		self.streamers[name] = streamer
		self.parent_conn, streamer.conn = Pipe()
		streamer.process = Process(target=streamer.run,args=())

		if streamer.duration > self.duration:
			self.duration = streamer.duration

	def run(self):

		for s in self.streamers.items():
			s.process.start()

		t0 = time()
		elapsed = 0.0

		self.ram,self.swap,self.t = [],[],[]

		while elapsed < self.duration:

			ram.append(psutil.virtual_memory().used / 1e9)
			swap.append(psutil.swap_memory().used / 1e9)
			t.append(time())
			wait(1)
			elapsed = t[-1] - t0

		for s in self.streamers.items():
			# see stream.report(). s is a local copy of
			# s that hasn't seen any of the changes since 
			# s.run()
			s.results = s.parent_conn.recv()

		for s in self.streamers.items():
			s.process.join()

	def save(self,fname):

		with h5py.File(self.fname,'a') as f:

			dset = f.create_dataset('ram_usage', data = self.ram)
			dset.attrs['units'] = 'GB'
			f.create_dataset('swap_usage', data = self.swap)
			dset = f.create_dataset('mem_sample_times', data = self.t)
			dset.attrs['unts'] = 'seconds'

			for name in self.streamers:

				r = self.streamers[name].results

				dset = f.create_dataset(name, data=r)
				dset.attrs['columns'] = 't_start, t_end, bytes_written'

class bolostream(object):
	"""
	Simulates an SPT3g bolometer data stream. New data is added to a buffer
	every write_period, and an io worker thread tries to write() the entire
	buffer every write period.

	Note that we actually fill the buffer with random bytes, to simulate the
	effect of (potentially high) RAM usage.
	"""

	def __init__(self, targetfs, payload_size, write_period, duration):

		stream.__init__(self, targetfs, payload_size, write_period, duration)

		self.io = Thread(target=self.io_run)
		self.buff = ''

	def run(self):

		self.io.start()
		t0 = time()
		elapsed = 0

		with open(self.fname,'a') as self.file:

			while elapsed < self.duration:

				self.buff += self.payload
				wait(self.write_period)

				elapsed = time() - t0 

			self.io.join()
			del self.buff


		self.report(np.array(self.results))

	def io_run(self):
		"""
		Worker function for the io thread. Writes the whole output buffer
		to the filesystem, and then sleep waits for new data.
		"""

		t0 = time()
		elapsed = 0

		t_start = None
		t_end = None
		bytes_written = 0

		while elapsed < self.duration:

			# the GIL properly locks self.buff for us
			if len(self.buff) > 0:

				t_start = time()
				self.file.write(self.buff)
				t_end = time()
				bytes_written = len(self.buff)
				self.buff = ''

				self.results.append( (t_start,t_end,bytes_written) )

			wait(.25 * self.write_period)

			elapsed = time() - t0 	

class gcpstream(object):

	def __init__(self, targetfs, payload_size, write_period, duration):

		stream.__init__(self, targetfs, payload_size, write_period, duration)

	def run(self):

		t0 = time()
		elapsed = 0

		t_start = None
		t_end = None
		bytes_written = self.payload_size

		with open(self.fname,'a') as self.file:

			while elapsed < self.duration:

				t_start = time()
				self.file.write(self.payload)
				t_end = time()

				self.results.append( (t_start,t_end,bytes_written) )

				dt = t_end - t_start

				if dt < self.write_period:

					wait(self.write_period - dt)

				elapsed = time() - t0

		self.report(np.array(self.results))





