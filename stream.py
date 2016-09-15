from multiprocessing import Process, Pipe
from threading import Thread, Lock
from time import time, sleep
from os import remove
from os.path import join
import random
import numpy as np
import h5py
import psutil
import os

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
		if self.conn:
			# if self.run was called by a stream_manager, it would
			# have assigned self.conn, and will expect to recv
			# self.results and save them to its process local version
			# of self's self.results. see stream_manager.run
			self.conn.send(results)

		self.results = results

class stream_manager(object):
	"""
	Manage multiple streams running in parallel. Just self.add() existing
	stream objects and then self.run() followed by self.save(fname)

	Also logs system memory usage while running. Manually setting
	self.duration and then calling self.run without adding any streams
	will give you just a memory usage logger.
	"""

	def __init__(self):

		self.streamers = {}
		self.ram,self.swap,self.t = [],[],[]

		self.duration = 0

	def add(self, streamer, name):

		self.streamers[name] = streamer
		streamer.parent_conn, streamer.conn = Pipe()
		streamer.process = Process(target=streamer.run,args=())

		if streamer.duration > self.duration:
			self.duration = streamer.duration

	def run(self,fname):
		try:
			self._run()
		finally:
			self._stop()
			self.save(fname)


	def _run(self):

		for k,s in self.streamers.iteritems():
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

	def _stop(self):

		for k,s in self.streamers.iteritems():
			# see stream.report(). s is a local copy of
			# s that hasn't seen any of the changes since 
			# s.run()
			s.results = s.parent_conn.recv()

		for k,s in self.streamers.iteritems():
			s.process.join()

	def save(self,fname):

		with h5py.File(fname,'a') as f:

			dset = f.create_dataset('ram_usage', data = self.ram)
			dset.attrs['units'] = 'GB'
			f.create_dataset('swap_usage', data = self.swap)
			dset = f.create_dataset('mem_sample_times', data = self.t)
			dset.attrs['unts'] = 'seconds'

			for name in self.streamers:

				r = self.streamers[name].results

				dset = f.create_dataset(name, data=r)
				dset.attrs['columns'] = 't_start, t_end, bytes_written'

class bolostream(stream):
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

		# ping pong between a data buffer and an io buffer to avoid
		# having the parent thread write to a buffer we're trying
		# to send to disk.
		self.buff = ['','']
		self.idx = 0
		self.io_lock = Lock()

	def run(self):

		self.io.start()
		t0 = time()
		elapsed = 0

		try:

			with open(self.fname,'a') as self.file:

				while elapsed < self.duration:

					with self.io_lock:

						self.buff[self.idx] += self.payload
						wait(self.write_period)

						elapsed = time() - t0

					
						#print '****** bolostream.run ******'
						#print self.buff
						#print '****** bolostream.run ******'


		finally:
			self.io.join()
			self.report(np.array(self.results))
			os.remove(self.fname)
			del self.buff
			self.buff = ['','']


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

			out = self.buff[self.idx]

			if len(out) > 0:

				with self.io_lock:

					print '------ bolostream.io_run ------'
					print self.buff
					print self.idx
					print '------ bolostream.io_run ------\n'

					# make sure self.run is using a different buffer
					# while this write is happening
					self.idx = int(not self.idx)

				t_start = time()
				self.file.write(out)
				t_end = time()
				bytes_written = len(out)
				out = ''

				print self.buff
				print ''

				self.results.append( (t_start,t_end,bytes_written) )

			wait(.25 * self.write_period)

			elapsed = time() - t0 	

class gcpstream(stream):

	def __init__(self, targetfs, payload_size, write_period, duration):

		stream.__init__(self, targetfs, payload_size, write_period, duration)

	def run(self):

		t0 = time()
		elapsed = 0

		t_start = None
		t_end = None
		bytes_written = self.payload_size

		try:

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

		finally:

			self.report(np.array(self.results))
			os.remove(self.fname)

