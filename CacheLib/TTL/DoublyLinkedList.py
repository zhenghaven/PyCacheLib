#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


from typing import Any, Optional

from .. import LockwSLD


class DoublyLinkedListNode(object):

	def __init__(self) -> None:
		super(DoublyLinkedListNode, self).__init__()

		self._prev: Optional[DoublyLinkedListNode] = None
		self._next: Optional[DoublyLinkedListNode] = None

		self.data: Any = None


class DoublyLinkedList(object):
	'''
	# DoublyLinkedList

	An implementation of a doubly linked list.
	Python does not have a built-in doubly linked list, to provide removal and
	insertion in O(1) time complexity, and even the built-in lru_cache in Python
	has to implement its own doubly linked list.
	(ref: https://github.com/python/cpython/blob/3.13/Lib/functools.py#L546)
	Meanwhile, the doubly linked list implemented by lru_cache is embedded in
	the _lru_cache_wrapper function, thus it is not reusable.
	'''

	def __init__(self) -> None:
		super(DoublyLinkedList, self).__init__()

		self.__storeLock = LockwSLD.LockwSLD()
		self.__head = DoublyLinkedListNode()
		self.__tail = DoublyLinkedListNode()

		self.__head._next = self.__tail
		self.__tail._prev = self.__head

	def __len__(self) -> int:
		with self.__storeLock:
			count = 0
			curr = self.__head._next
			while curr != self.__tail:
				count += 1
				curr = curr._next
			return count

	def _iterLockHeld(self):
		curr = self.__head._next
		while curr != self.__tail:
			yield curr.data
			curr = curr._next

	def __iter__(self):
		with self.__storeLock:
			l = list(self._iterLockHeld())

		for item in l:
			yield item

	def _reversedLockHeld(self):
		curr = self.__tail._prev
		while curr != self.__head:
			yield curr.data
			curr = curr._prev

	def __reversed__(self):
		with self.__storeLock:
			l = list(self._reversedLockHeld())

		for item in l:
			yield item

	def __contains__(self, data: Any) -> bool:
		with self.__storeLock:
			for item in self._iterLockHeld():
				if item == data:
					return True

			return False

	def __str__(self) -> str:
		return f'{self.__class__.__name__}({list(self)})'

	def __repr__(self) -> str:
		return self.__str__()

	@classmethod
	def _InsertNodeLeftLockHeld(
		cls,
		node: DoublyLinkedListNode,
		insertPos: DoublyLinkedListNode,
	) -> None:
		'''
		Insert a node to the left of the insertPos node.
		'''
		node._prev = insertPos._prev
		node._next = insertPos

		insertPos._prev._next = node
		insertPos._prev = node

	def append(self, data: Any) -> DoublyLinkedListNode:
		with self.__storeLock:
			newNode = DoublyLinkedListNode()
			newNode.data = data

			self._InsertNodeLeftLockHeld(newNode, self.__tail)

			return newNode

	@classmethod
	def _InsertNodeRightLockHeld(
		cls,
		node: DoublyLinkedListNode,
		insertPos: DoublyLinkedListNode,
	) -> None:
		'''
		Insert a node to the right of the insertPos node.
		'''
		node._prev = insertPos
		node._next = insertPos._next

		insertPos._next._prev = node
		insertPos._next = node

	def appendleft(self, data: Any) -> DoublyLinkedListNode:
		with self.__storeLock:
			newNode = DoublyLinkedListNode()
			newNode.data = data

			self._InsertNodeRightLockHeld(newNode, self.__head)

			return newNode

	def _emptyLockHeld(self) -> bool:
		return self.__head._next == self.__tail

	@classmethod
	def _RemoveLockHeld(cls, node: DoublyLinkedListNode) -> None:
		node._prev._next = node._next
		node._next._prev = node._prev

		node._prev = None
		node._next = None

	def pop(self) -> Any:
		with self.__storeLock:
			if self._emptyLockHeld():
				raise IndexError('pop from an empty list')

			node = self.__tail._prev
			self._RemoveLockHeld(node)

			return node.data

	def popleft(self) -> Any:
		with self.__storeLock:
			if self._emptyLockHeld():
				raise IndexError('pop from an empty list')

			node = self.__head._next
			self._RemoveLockHeld(node)

			return node.data

	def front(self) -> Any:
		with self.__storeLock:
			if self._emptyLockHeld():
				raise IndexError('peek from an empty list')

			return self.__head._next.data

	def back(self) -> Any:
		with self.__storeLock:
			if self._emptyLockHeld():
				raise IndexError('peek from an empty list')

			return self.__tail._prev.data

	def empty(self) -> bool:
		with self.__storeLock:
			return self._emptyLockHeld()

	def remove(self, node: DoublyLinkedListNode) -> None:
		with self.__storeLock:
			self._RemoveLockHeld(node)

	def removeappend(self, node: DoublyLinkedListNode) -> None:
		with self.__storeLock:
			self._RemoveLockHeld(node)
			self._InsertNodeLeftLockHeld(node, self.__tail)

	def removeappendleft(self, node: DoublyLinkedListNode) -> None:
		with self.__storeLock:
			self._RemoveLockHeld(node)
			self._InsertNodeRightLockHeld(node, self.__head)

