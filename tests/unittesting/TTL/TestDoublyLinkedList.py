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
import unittest

from CacheLib.TTL.DoublyLinkedList import DoublyLinkedList


class TestDoublyLinkedList(unittest.TestCase):

	def setUp(self):
		self.dllClsName = f'{DoublyLinkedList.__name__}'

	def tearDown(self):
		pass

	def test_TTL_DoublyLinkedList_01AppendAndLen(self):
		dll = DoublyLinkedList()
		self.assertEqual(len(dll), 0)

		dll.append(1)
		self.assertEqual(len(dll), 1)
		dll.append(2)
		self.assertEqual(len(dll), 2)

		dll.appendleft(3)
		self.assertEqual(len(dll), 3)
		dll.appendleft(4)
		self.assertEqual(len(dll), 4)

	def test_TTL_DoublyLinkedList_02Iter(self):
		dll = DoublyLinkedList()
		dll.append(1)
		dll.append(2)
		dll.append(3)
		dll.append(4)

		i = 1
		for item in dll:
			self.assertEqual(item, i)
			i += 1

		self.assertEqual(i, 5)

		i -= 1 # back to 4
		for item in reversed(dll):
			self.assertEqual(item, i)
			i -= 1

		self.assertEqual(i, 0)

	def test_TTL_DoublyLinkedList_03Str(self):
		dll = DoublyLinkedList()
		dll.append(1)
		dll.append(2)
		dll.append(3)
		dll.append(4)

		self.assertEqual(str(dll), f'{self.dllClsName}([1, 2, 3, 4])')
		self.assertEqual(repr(dll), f'{self.dllClsName}([1, 2, 3, 4])')

		dll.appendleft(0)
		self.assertEqual(str(dll), f'{self.dllClsName}([0, 1, 2, 3, 4])')
		self.assertEqual(repr(dll), f'{self.dllClsName}([0, 1, 2, 3, 4])')

	def test_TTL_DoublyLinkedList_04Pop(self):
		dll = DoublyLinkedList()
		dll.append(1)
		dll.append(2)
		dll.append(3)
		dll.append(4)

		self.assertEqual(dll.pop(), 4)
		self.assertEqual(dll.pop(), 3)
		self.assertEqual(dll.pop(), 2)
		self.assertEqual(dll.pop(), 1)

		self.assertEqual(len(dll), 0)

		dll.append(1)
		dll.append(2)
		dll.append(3)
		dll.append(4)

		self.assertEqual(dll.popleft(), 1)
		self.assertEqual(dll.popleft(), 2)
		self.assertEqual(dll.popleft(), 3)
		self.assertEqual(dll.popleft(), 4)

		self.assertEqual(len(dll), 0)

	def test_TTL_DoublyLinkedList_05Remove(self):
		dll = DoublyLinkedList()
		node1 = dll.append(1)
		node2 = dll.append(2)
		node3 = dll.append(3)
		node4 = dll.append(4)

		dll.remove(node2)
		self.assertEqual(len(dll), 3)
		self.assertEqual(str(dll), f'{self.dllClsName}([1, 3, 4])')

		dll.remove(node1)
		self.assertEqual(len(dll), 2)
		self.assertEqual(str(dll), f'{self.dllClsName}([3, 4])')

		dll.remove(node4)
		self.assertEqual(len(dll), 1)
		self.assertEqual(str(dll), f'{self.dllClsName}([3])')

		dll.remove(node3)
		self.assertEqual(len(dll), 0)
		self.assertEqual(str(dll), f'{self.dllClsName}([])')

	def test_TTL_DoublyLinkedList_06RemoveRandom(self):
		numOfItems = 1000
		l = [ random.randint(0, 10**9) for _ in range(numOfItems) ]
		dll = DoublyLinkedList()
		nodes = [ dll.append(v) for v in l ]

		while len(nodes) > 0:
			# pick a random node to remove
			removeIdx = random.randint(0, len(nodes) - 1)
			nodeToRemove = nodes[removeIdx]
			nodes = nodes[:removeIdx] + nodes[removeIdx + 1:]

			dll.remove(nodeToRemove)

			for nodeInList, dataInDll in zip(nodes, dll):
				self.assertEqual(nodeInList.data, dataInDll)

	def test_TTL_DoublyLinkedList_07ThreadSafe(self):
		numOfThreads = 10
		numOfItemsPerThread = 1000

		itemsForThreads = [
			[
				num for num in
					range(
						thrIdx       * numOfItemsPerThread,
						(thrIdx + 1) * numOfItemsPerThread
					)
			]
			for thrIdx in range(numOfThreads)
		]
		for items in itemsForThreads:
			assert len(items) == numOfItemsPerThread
		assert len(set(sum(itemsForThreads, start=[]))) == numOfThreads * numOfItemsPerThread

		dll = DoublyLinkedList()
		appendMethods = [ dll.append, dll.appendleft ]

		startSignal = threading.Event()

		def AppendAndRemoveItems(items):
			startSignal.wait()

			# add all onces
			random.shuffle(items)
			nodes = [ random.choice(appendMethods)(v) for v in items ]
			# ensure all items are in the list
			for item in items:
				self.assertIn(item, dll)
			# remove all items
			random.shuffle(nodes)
			for node in nodes:
				dll.remove(node)
			# ensure all items are removed
			for item in items:
				self.assertNotIn(item, dll)

			# add and remove again
			random.shuffle(items)
			for item in items:
				# append
				node = random.choice(appendMethods)(item)
				self.assertIn(item, dll)
				# remove
				dll.remove(node)
				self.assertNotIn(item, dll)

		threads = [
			threading.Thread(
				target=AppendAndRemoveItems,
				args=(items,),
			)
			for items in itemsForThreads
		]
		for th in threads:
			th.start()

		# start the threads
		startSignal.set()

		# wait for all threads to finish
		for th in threads:
			th.join()

		# ensure the list is empty
		self.assertEqual(len(dll), 0)

