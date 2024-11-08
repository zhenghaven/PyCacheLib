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
import uuid

from typing import Any

from CacheLib.TTL.MultiKeyUniTTLValueCache import MultiKeyUniTTLValueCache
from CacheLib.TTL.Interfaces import KeyValueKey, KeyValueItem


class TestItemClass(KeyValueItem):

	def __init__(
		self,
		keys: list[KeyValueKey],
		value: Any,
	) -> None:
		super(TestItemClass, self).__init__()

		self.keys = keys
		self.value = value

		self.isTerminated = threading.Event()

	def Terminate(self) -> None:
		self.isTerminated.set()

	def GetKeys(self) -> list[KeyValueKey]:
		return self.keys


class TestMulKeyUniTTLValue(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass


	def test_TTL_MulKeyUniTTLValue_01TestItemClass(self):
		keys = [ 12345, 'abc', ]
		item = TestItemClass(keys=keys, value='value')
		self.assertEqual(item.GetKeys(), keys)
		self.assertEqual(item.value, 'value')

		with self.assertRaises(NotImplementedError):
			item.GetTTL()

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

	def FetchExpiredValue(self, cache, keys, oriItem) -> None:
		for key in keys:
			self.assertFalse(key in cache)
			item = cache.Get(key)
			self.assertIsNone(item)

		self.assertTrue(oriItem.isTerminated.is_set())

	def FetchValues(self, cache, kvs) -> None:
		for keys, expValue in kvs:
			self.FetchValue(cache, keys, expValue)


	def test_TTL_MulKeyUniTTLValue_02Basics(self):
		cache = MultiKeyUniTTLValueCache(ttl=(500, 'ms'))


		# t = 0.0


		# Put in a value, which will expire @ t = 0.5
		keys1 = [ 12345, 'abc', ]
		val1 = 'value1'
		item1 = TestItemClass(keys=keys1, value=val1)
		cache.Put(item1) # , debugLogTimestamp=True

		# and we can fetch it
		self.FetchValue(cache, keys1, val1) # item1 will expire @ t = 0.5

		# there are 1 item and 2 keys in the cache
		self.assertEqual(len(cache), 1)
		self.assertEqual(cache.NumOfKeys(), 2)


		# wait for ttl/2
		time.sleep(0.25) # t = 0.25


		# put in another value, which will expire @ t = 0.75
		keys2 = [ 54321, 'cba', ]
		val2 = 'value2'
		item2 = TestItemClass(keys=keys2, value=val2)
		cache.Put(item2) # , debugLogTimestamp=True

		# and we can fetch both values
		self.FetchValues(
			cache,
			[
				(keys1, val1), # item1 will expire @ t = 0.75
				(keys2, val2), # item2 will expire @ t = 0.75
			]
		)
		# there are 2 items and 4 keys in the cache
		self.assertEqual(len(cache), 2)
		self.assertEqual(cache.NumOfKeys(), 4)

		# try to put in a value with a key that already exists
		keys3 = [ 12353234, 'jhrotig', 'abc', ]
		val3 = 'value3'
		item3 = TestItemClass(keys=keys3, value=val3)
		with self.assertRaises(KeyError):
			cache.Put(item3)
		# none of the non-existing keys are in the cache
		self.assertFalse(keys3[0] in cache)
		self.assertFalse(keys3[1] in cache)
		self.assertTrue(keys3[2] in cache)
		# and we can't get values using these keys
		self.assertIsNone(cache.Get(keys3[0]))
		self.assertIsNone(cache.Get(keys3[1]))

		# and we can still get the existing value back
		self.FetchValues(
			cache,
			[
				(keys1, val1), # item1 will expire @ t = 0.75
				(keys2, val2), # item2 will expire @ t = 0.75
			]
		)
		# there are 2 items and 4 keys in the cache
		self.assertEqual(len(cache), 2)
		self.assertEqual(cache.NumOfKeys(), 4)


		# wait for first item to expire
		time.sleep(0.25) # t = 0.5


		# fetch item1 to update its expiry time
		self.FetchValue(cache, keys1, val1) # item1 will expire @ t = 1.0


		# wait for item2 to expire
		time.sleep(0.35) # t = 0.85


		# now, we can no longer get item2 back
		self.FetchExpiredValue(cache, keys2, item2)

		# and we can still fetch item1
		self.FetchValue(cache, keys1, val1) # item1 will expire @ t = 1.35
		# there are 1 item and 2 keys in the cache
		self.assertEqual(len(cache), 1)
		self.assertEqual(cache.NumOfKeys(), 2)


		# wait for item1 to expire
		time.sleep(0.65) # t = 1.5


		# and we can no longer get item1 back
		self.FetchExpiredValue(cache, keys1, item1)

		# there are 0 items and 0 keys in the cache
		self.assertEqual(len(cache), 0)
		self.assertEqual(cache.NumOfKeys(), 0)


	def test_TTL_MulKeyUniTTLValue_03CleanUp(self):
		cache = MultiKeyUniTTLValueCache(ttl=(500, 'ms'))

		keys1 = [ 12345, 'abc', ]
		val1 = 'value1'
		item1 = TestItemClass(keys=keys1, value=val1)

		keys2 = [ 54321, 'cba', ]
		val2 = 'value2'
		item2 = TestItemClass(keys=keys2, value=val2)

		keys3 = [ 6655443, 'srhbrew', ]
		val3 = 'value3'
		item3 = TestItemClass(keys=keys3, value=val3)

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
		self.FetchExpiredValue(cache, keys1, item1)
		self.FetchExpiredValue(cache, keys2, item2)
		self.FetchExpiredValue(cache, keys3, item3)
		# there are 0 items and 0 keys in the cache
		self.assertEqual(len(cache), 0)
		self.assertEqual(cache.NumOfKeys(), 0)


	def GenerateAndFetchValues(
		self,
		cache: MultiKeyUniTTLValueCache,
		numRounds: int,
		numItems: int,
		numKeys: int,
		roundDelay: float,
		startSignal: threading.Event = None,
		doneEvent: threading.Event = None,
		itemsStore: list[TestItemClass] = None,
	) -> None:
		if startSignal is not None:
			startSignal.wait()

		for _ in range(numRounds):
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
				item = TestItemClass(keys=keys, value=val)

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

			time.sleep(roundDelay)

		if doneEvent is not None:
			doneEvent.set()


	def test_TTL_MulKeyUniTTLValue_04MultiThreading(self):
		cache = MultiKeyUniTTLValueCache(ttl=(500, 'ms'))

		numThreads = 10
		numRoundsPerThread = 10
		numItemsPerRound = 10

		itemsStore = list()
		startSignal = threading.Event()
		doneEvents = [
			threading.Event()
			for _ in range(numThreads)
		]
		threads = [
			threading.Thread(
				target=self.GenerateAndFetchValues,
				kwargs=dict(
					cache=cache,
					numRounds=numRoundsPerThread,
					numItems=numItemsPerRound,
					numKeys=10,
					roundDelay=0.1,
					startSignal=startSignal,
					doneEvent=doneEvents[i],
					itemsStore=itemsStore,
				),
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
		time.sleep(0.6)

		# all items are removed from the cache
		self.assertEqual(len(cache), 0)
		self.assertEqual(cache.NumOfKeys(), 0)

		# and they all are terminated
		self.assertEqual(
			len(itemsStore),
			numThreads * numRoundsPerThread * numItemsPerRound
		)
		for item in itemsStore:
			self.assertTrue(item.isTerminated.is_set())

		# clean up the threads
		for th in threads:
			th.join()

