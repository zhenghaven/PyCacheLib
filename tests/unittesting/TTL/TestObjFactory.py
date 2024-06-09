#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


import threading
import time
import unittest

from CacheLib.TTL.ObjFactoryCache import ObjFactoryCache
from CacheLib.TTL.Interfaces import Terminable


class TestItemClass(Terminable):

	def __init__(
		self,
		arg1: int,
		arg2: str,
	) -> None:
		super(TestItemClass, self).__init__()

		self.arg1 = arg1
		self.arg2 = arg2

		self.isTerminated = threading.Event()
		self.isExited = threading.Event()

	def Work(self) -> None:
		while (not self.isTerminated.is_set()) and (not self.isExited.is_set()):
			time.sleep(0.1)
		# print(f'Work done: {self.arg1} {self.arg2}')

	def Terminate(self) -> None:
		self.isTerminated.set()


def FetchWorkAndPut(cache: ObjFactoryCache, hasPut: threading.Event) -> None:
	inst: TestItemClass = cache.Get()
	inst.Work()
	cache.Put(inst)
	hasPut.set()


class TestObjFactory(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_ObjFactory_1Basics(self):
		objFactoryCache = ObjFactoryCache(
			ttl=1.0,
			objCls=TestItemClass,
			objArgs=(1,),
			objKwargs={'arg2': 'test'},
		)

		# get an instance
		inst1 = objFactoryCache.Get()
		self.assertEqual(inst1.arg1, 1)
		self.assertEqual(inst1.arg2, 'test')
		self.assertFalse(inst1.isTerminated.is_set())
		self.assertEqual(len(objFactoryCache), 1)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

		# get another instance
		inst2 = objFactoryCache.Get()
		self.assertEqual(inst2.arg1, 1)
		self.assertEqual(inst2.arg2, 'test')
		self.assertFalse(inst2.isTerminated.is_set())
		self.assertEqual(len(objFactoryCache), 2)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)
		# inst2 != inst1
		self.assertNotEqual(inst2.IDInt, inst1.IDInt)

		# put the 1st instance back
		objFactoryCache.Put(inst1)
		self.assertFalse(inst1.isTerminated.is_set())
		self.assertEqual(len(objFactoryCache), 2)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 1)

		time.sleep(0.6) # t = 0.6

		# put the 2nd instance back
		objFactoryCache.Put(inst2)
		self.assertFalse(inst2.isTerminated.is_set())
		self.assertEqual(len(objFactoryCache), 2)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 2)

		time.sleep(0.6) # t = 1.2

		# the 1st instance should be expired, but has not been cleaned up
		self.assertFalse(inst1.isTerminated.is_set())

		# check the number of idle instances
		self.assertEqual(len(objFactoryCache), 1)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 1)
		# the 1st instance should be expired and cleaned up
		self.assertTrue(inst1.isTerminated.is_set())
		# the 2nd instance should still be valid
		self.assertFalse(inst2.isTerminated.is_set())

		time.sleep(0.6) # t = 1.8

		# the second instance should be expired, but has not been cleaned up
		self.assertFalse(inst2.isTerminated.is_set())

		# get a new instance
		inst3 = objFactoryCache.Get()
		self.assertEqual(inst3.arg1, 1)
		self.assertEqual(inst3.arg2, 'test')
		self.assertFalse(inst3.isTerminated.is_set())
		self.assertEqual(len(objFactoryCache), 1)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)
		# inst3 != inst1
		self.assertNotEqual(inst3.IDInt, inst1.IDInt)
		# inst3 != inst2
		self.assertNotEqual(inst3.IDInt, inst2.IDInt)

		# the second instance should be expired and cleaned up
		self.assertTrue(inst2.isTerminated.is_set())

		# Terminate the factory
		objFactoryCache.Terminate()
		self.assertEqual(len(objFactoryCache), 0)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

	def test_ObjFactory_2Threading(self):
		objFactoryCache = ObjFactoryCache(
			ttl=1.0,
			objCls=TestItemClass,
			objArgs=(1,),
			objKwargs={'arg2': 'test'},
		)

		numOfThreads = 10

		hasPuts = [ threading.Event() for _ in range(numOfThreads) ]
		threads = [
			threading.Thread(
				target=FetchWorkAndPut,
				args=(objFactoryCache, hasPuts[i]),
			)
			for i in range(numOfThreads)
		]
		for th in threads:
			th.start()

		self.assertEqual(len(objFactoryCache), numOfThreads)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

		# get items
		items = [
			item for idInt, item in objFactoryCache.inUseObjs.items()
		]
		self.assertEqual(len(items), numOfThreads)
		for item in items:
			self.assertFalse(item.isTerminated.is_set())
			self.assertFalse(item.isExited.is_set())

		# exit first half of the threads
		for item in items[:numOfThreads//2]:
			item.isExited.set()
			# print(f'Exiting: {item.arg1} {item.arg2}')

		# wait for the first half of the threads to finish
		tStart = time.time()
		while len([ 1 for hasPut in hasPuts if hasPut.is_set() ]) < numOfThreads//2:
			time.sleep(0.1)
			# print(len([ 1 for hasPut in hasPuts if hasPut.is_set() ]))
			# max wait time: 5 seconds
			self.assertLess(time.time() - tStart, 5.0)

		self.assertEqual(len(objFactoryCache), numOfThreads)
		self.assertEqual(objFactoryCache.NumberOfIdle(), numOfThreads//2)

		time.sleep(1.1) # t = ~1.1

		# half of the items should be expired and cleaned up
		self.assertEqual(len(objFactoryCache), numOfThreads//2)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)
		self.assertEqual(
			len([ 1 for item in items if item.isTerminated.is_set() ]),
			numOfThreads//2
		)

		# exit the remaining threads
		for item in items[numOfThreads//2:]:
			item.isExited.set()

		# wait for the remaining threads to finish
		tStart = time.time()
		while len([ 1 for hasPut in hasPuts if hasPut.is_set() ]) < numOfThreads:
			time.sleep(0.1)
			# max wait time: 5 seconds
			self.assertLess(time.time() - tStart, 5.0)

		self.assertEqual(len(objFactoryCache), numOfThreads//2)
		self.assertEqual(objFactoryCache.NumberOfIdle(), numOfThreads//2)

		time.sleep(1.1) # t = ~2.2

		# all items should be expired and cleaned up
		self.assertEqual(len(objFactoryCache), 0)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)
		self.assertEqual(
			len([ 1 for item in items if item.isTerminated.is_set() ]),
			numOfThreads
		)

		# Terminate the factory
		objFactoryCache.Terminate()
		self.assertEqual(len(objFactoryCache), 0)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

		# clean up the threads
		for th in threads:
			th.join()

	def test_ObjFactory_3Untrack(self):
		objFactoryCache = ObjFactoryCache(
			ttl=1.0,
			objCls=TestItemClass,
			objArgs=(1,),
			objKwargs={'arg2': 'test'},
		)

		# construct 2 instances, one tracked, one untracked
		inst1 = objFactoryCache.Get(trackInUse=False)
		inst2 = objFactoryCache.Get(trackInUse=True)
		self.assertEqual(len(objFactoryCache), 1)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

		# untrack the tracked instance
		objFactoryCache.Untrack(inst2)
		self.assertEqual(len(objFactoryCache), 0)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

		# Terminate the factory
		objFactoryCache.Terminate()
		self.assertEqual(len(objFactoryCache), 0)
		self.assertEqual(objFactoryCache.NumberOfIdle(), 0)

		# both instances shouldn't be terminated
		self.assertFalse(inst1.isTerminated.is_set())
		self.assertFalse(inst2.isTerminated.is_set())

		# terminate the instances
		inst1.Terminate()
		inst2.Terminate()

