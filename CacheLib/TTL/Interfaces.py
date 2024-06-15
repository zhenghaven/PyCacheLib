#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


import logging
import uuid

from collections.abc import Hashable
from typing import List


class IDObject(object):

	def __init__(self) -> None:
		super(IDObject, self).__init__()

		self.__id = uuid.uuid4()

	@property
	def IDInt(self) -> int:
		'''
		### IDInt - Get the ID as an integer
		'''
		return int(self.__id.int)


class Terminable(IDObject):

	def __init__(self) -> None:
		super(Terminable, self).__init__()

	def Terminate(self) -> None:
		'''
		### Terminate - Terminate the object
		'''
		raise NotImplementedError('Not implemented')


class KeyValueKey(Hashable):
	'''
	## KeyValueKey - Key for key-value map cache
	'''
	pass


class KeyValueItem(Terminable):
	'''
	## KeyValueItem - Items that are stored in a key-value map cache
	'''

	def __init__(self) -> None:
		super(KeyValueItem, self).__init__()

	def GetKeys(self) -> List[KeyValueKey]:
		'''
		### GetKeys - Get the keys of the item

		Our key-value cache supports multiple keys for each item.
		However, it is required that the keys are unique in all value space;
		in other words, 1 key should always only map to 1 value, and
		NO 2 keys should map to the same value.

		The subclass is responsible for ensuring this; otherwise, non-unique
		keys will cause undefined behavior.
		'''
		raise NotImplementedError('Not implemented')

	def GetTTL(self) -> float:
		'''
		### GetTTL - Get the time-to-live of the item
		'''
		raise NotImplementedError('Not implemented')

	def GetTTLNanoSec(self) -> int:
		'''
		### GetTTLNanoSec - Get the time-to-live of the item in nanoseconds
		'''
		return int(self.GetTTL() * 1e9)


class TTL(Terminable):
	'''
	## TTL
	a generic Time-to-live cache base class
	'''

	def __init__(self) -> None:
		super(TTL, self).__init__()

		self.logger = logging.getLogger(
			f'{__name__}.{self.__class__.__name__}'
		)

	def CleanUpExpired(self) -> None:
		'''
		### CleanUpExpired - Clean up expired items
		'''
		raise NotImplementedError('Not implemented')


class ClassWideTTL(TTL):
	'''
	## ClassWideTTL
	a generic Time-to-live cache base class with a class-wide defined TTL value
	'''

	def __init__(self, ttl: float) -> None:
		super(ClassWideTTL, self).__init__()

		self.ttl = ttl

	def CleanUpExpired(self) -> None:
		'''
		### CleanUpExpired - Clean up expired items
		'''
		raise NotImplementedError('Not implemented')

