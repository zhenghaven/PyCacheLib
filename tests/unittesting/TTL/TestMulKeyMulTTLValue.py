#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


import random
import threading
import time
import unittest
import uuid

from typing import Any

from CacheLib.TTL.MultiKeyMultiTTLValueCache import MultiKeyMultiTTLValueCache
from CacheLib.TTL.Interfaces import KeyValueKey, KeyValueItem


class TestItemClass(KeyValueItem):

	def __init__(
		self,
		keys: list[KeyValueKey],
		value: Any,
		ttl: float,
	) -> None:
		super(TestItemClass, self).__init__()

		self.keys = keys
		self.value = value
		self.ttl = ttl

		self.isTerminated = threading.Event()

	def Terminate(self) -> None:
		self.isTerminated.set()

	def GetKeys(self) -> list[KeyValueKey]:
		return self.keys

	def GetTTL(self) -> float:
		return self.ttl


class TestMulKeyMulTTLValue(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_TTL_MulKeyMulTTLValue_01TestItemClass(self):
		keys = [ 12345, 'abc', ]
		item = TestItemClass(keys=keys, value='value', ttl=1.0)
		self.assertEqual(item.GetKeys(), keys)
		self.assertEqual(item.GetTTL(), 1.0)
		self.assertEqual(item.GetTTLNanoSec(), 1000000000)
		self.assertEqual(type(item.GetTTLNanoSec()), int)
		self.assertEqual(item.value, 'value')

		self.assertFalse(item.isTerminated.is_set())
		item.Terminate()
		self.assertTrue(item.isTerminated.is_set())

	def FetchValue(self, cache, keys, expValue) -> None:
		for key in keys:
			self.assertTrue(key in cache)
			item = cache.Get(key)
			self.assertEqual(item.GetKeys(), keys)
			self.assertEqual(item.value, expValue)
			self.assertFalse(item.isTerminated.is_set())

	def FetchValues(self, cache, kvs) -> None:
		for keys, expValue in kvs:
			self.FetchValue(cache, keys, expValue)

	def GenerateAndFetchValues(
		self,
		cache: MultiKeyMultiTTLValueCache,
		numItems: int,
		numKeys: int,
		maxTtl = 3.0,
		startSignal: threading.Event = None,
		doneEvent: threading.Event = None,
		itemsStore: list[TestItemClass] = None,
	) -> None:
		if startSignal is not None:
			startSignal.wait()

		kvs = []
		for _ in range(numItems):
			# build the keys and value
			keys = [ ]
			for j in range(numKeys):
				if j % 3 == 0:
					keys.append(uuid.uuid4())
				elif j % 2 == 0:
					keys.append(uuid.uuid4().int)
				else:
					keys.append(uuid.uuid4().hex)
			val = uuid.uuid4().hex
			ttl = random.uniform(1.0, maxTtl)
			item = TestItemClass(keys=keys, value=val, ttl=ttl)

			# put the item in the cache
			cache.Put(item)
			kvs.append((keys, val))

			# store the item
			if itemsStore is not None:
				itemsStore.append(item)

			# fetch the value
			self.FetchValue(cache, keys, val)

		# fetch all values
		self.FetchValues(cache, kvs)

		if doneEvent is not None:
			doneEvent.set()

	def test_TTL_MulKeyMulTTLValue_02Basics(self):
		cache = MultiKeyMultiTTLValueCache()

		# Put in the value
		keys1 = [ 12345, 'abc', ]
		val1 = 'value1'
		item1 = TestItemClass(keys=keys1, value=val1, ttl=2.0)
		cache.Put(item1)

		# and we can fetch it
		self.FetchValue(cache, keys1, val1)

		# there are 1 item and 2 keys in the cache
		self.assertEqual(len(cache), 1)
		self.assertEqual(cache.NumOfKeys(), 2)

		# put in another value
		keys2 = [ 54321, 'cba', ]
		val2 = 'value2'
		item2 = TestItemClass(keys=keys2, value=val2, ttl=1.0)
		cache.Put(item2)

		# and we can fetch both values
		self.FetchValues(
			cache,
			[
				(keys1, val1),
				(keys2, val2),
			]
		)

		# there are 2 items and 4 keys in the cache
		self.assertEqual(len(cache), 2)
		self.assertEqual(cache.NumOfKeys(), 4)

		# try to put in a value with a key that already exists
		keys3 = [ 12353234, 'jhrotig', 6655443, 'abc', ]
		val3 = 'value3'
		item3 = TestItemClass(keys=keys3, value=val3, ttl=3.0)
		with self.assertRaises(KeyError):
			cache.Put(item3)
		# none of the non-existing keys are in the cache
		self.assertFalse(keys3[0] in cache)
		self.assertFalse(keys3[1] in cache)
		self.assertFalse(keys3[2] in cache)
		self.assertTrue(keys3[3] in cache)
		# and we can't get values using these keys
		self.assertIsNone(cache.Get(keys3[0]))
		self.assertIsNone(cache.Get(keys3[1]))
		self.assertIsNone(cache.Get(keys3[2]))

		# and we can still get the first value back
		self.FetchValues(
			cache,
			[
				(keys1, val1),
				(keys2, val2),
			]
		)

		# there are 2 items and 4 keys in the cache
		self.assertEqual(len(cache), 2)
		self.assertEqual(cache.NumOfKeys(), 4)

		# wait for item2 to expire
		time.sleep(1.1) # t = 1.1

		# now, we can no longer get item2 back
		item = cache.Get(keys2[0])
		self.assertIsNone(item)
		item = cache.Get(keys2[1])
		self.assertIsNone(item)
		# and they are not in the cache
		self.assertFalse(keys2[0] in cache)
		self.assertFalse(keys2[1] in cache)
		# and that item2 is terminated
		self.assertTrue(item2.isTerminated.is_set())

		# and we can still fetch item1
		self.FetchValue(cache, keys1, val1)

		# there are 1 item and 2 keys in the cache
		self.assertEqual(len(cache), 1)
		self.assertEqual(cache.NumOfKeys(), 2)

		# wait for item1 to expire
		time.sleep(1.1) # t = 2.2

		# and we can no longer get item1 back
		item = cache.Get(keys1[0])
		self.assertIsNone(item)
		item = cache.Get(keys1[1])
		self.assertIsNone(item)
		# and they are not in the cache
		self.assertFalse(keys1[0] in cache)
		self.assertFalse(keys1[1] in cache)
		# and that item1 is terminated
		self.assertTrue(item1.isTerminated.is_set())

		# there are 0 items and 0 keys in the cache
		self.assertEqual(len(cache), 0)
		self.assertEqual(cache.NumOfKeys(), 0)

	def test_TTL_MulKeyMulTTLValue_03CleanUp(self):
		cache = MultiKeyMultiTTLValueCache()

		keys1 = [ 12345, 'abc', ]
		val1 = 'value1'
		item1 = TestItemClass(keys=keys1, value=val1, ttl=1.0)

		keys2 = [ 54321, 'cba', ]
		val2 = 'value2'
		item2 = TestItemClass(keys=keys2, value=val2, ttl=2.0)

		keys3 = [ 6655443, 'srhbrew', ]
		val3 = 'value3'
		item3 = TestItemClass(keys=keys3, value=val3, ttl=3.0)

		cache.Put(item1)
		cache.Put(item2)
		cache.Put(item3)

		# there are 3 items and 6 keys in the cache
		self.assertEqual(len(cache), 3)
		self.assertEqual(cache.NumOfKeys(), 6)

		self.FetchValues(
			cache,
			[
				(keys1, val1),
				(keys2, val2),
				(keys3, val3),
			]
		)

		# manually terminate the cache
		cache.Terminate()

		# all items are removed from the cache
		self.assertEqual(len(cache), 0)
		self.assertEqual(cache.NumOfKeys(), 0)

		# and they are not in the cache
		self.assertFalse(keys1[0] in cache)
		self.assertFalse(keys1[1] in cache)
		self.assertFalse(keys2[0] in cache)
		self.assertFalse(keys2[1] in cache)
		self.assertFalse(keys3[0] in cache)
		self.assertFalse(keys3[1] in cache)

		# and we can't get values using these keys
		self.assertIsNone(cache.Get(keys1[0]))
		self.assertIsNone(cache.Get(keys1[1]))
		self.assertIsNone(cache.Get(keys2[0]))
		self.assertIsNone(cache.Get(keys2[1]))
		self.assertIsNone(cache.Get(keys3[0]))
		self.assertIsNone(cache.Get(keys3[1]))

		# and that all items are terminated
		self.assertTrue(item1.isTerminated.is_set())
		self.assertTrue(item2.isTerminated.is_set())
		self.assertTrue(item3.isTerminated.is_set())

	def test_TTL_MulKeyMulTTLValue_04MultiThreading(self):
		cache = MultiKeyMultiTTLValueCache()

		numThreads = 10
		maxTTL = 3.0

		itemsStore = list()
		startSignal = threading.Event()
		doneEvents = [
			threading.Event()
			for _ in range(numThreads)
		]
		threads = [
			threading.Thread(
				target=self.GenerateAndFetchValues,
				args=(cache, 10, 10, maxTTL, startSignal, doneEvents[i], itemsStore),
			)
			for i in range(numThreads)
		]
		for th in threads:
			th.start()

		# start the threads
		startSignal.set()

		# wait for all threads to finish
		for de in doneEvents:
			de.wait()

		# wait for all cache items to expire
		time.sleep(maxTTL + 0.1)

		# all items are removed from the cache
		self.assertEqual(len(cache), 0)
		self.assertEqual(cache.NumOfKeys(), 0)

		# and they all are terminated
		for item in itemsStore:
			self.assertTrue(item.isTerminated.is_set())

		# clean up the threads
		for th in threads:
			th.join()

