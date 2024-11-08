#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


import time

from typing import Dict, List, Tuple, Union

from .. import LockwSLD
from . import DoublyLinkedList
from .Interfaces import KeyValueKey, KeyValueItem, TTL


class MultiKeyUniTTLValueCache(TTL):

	@classmethod
	def _CalcNanoSecFromInput(
		self,
		ttl: Tuple[int, str],
	) -> int:
		ttlVal, ttlUnit = ttl
		if ttlUnit == 'ns':
			return ttlVal
		elif ttlUnit == 'us':
			return ttlVal * 1000
		elif ttlUnit == 'ms':
			return ttlVal * 1000000
		elif ttlUnit == 's':
			return ttlVal * 1000000000
		else:
			raise ValueError(f'Invalid TTL unit: {ttlUnit}')

	def __init__(
		self,
		ttl: Tuple[int, str],
	) -> None:
		super(MultiKeyUniTTLValueCache, self).__init__()

		self.__ttlNanoSec = self._CalcNanoSecFromInput(ttl)

		self.__storeLock = LockwSLD.LockwSLD()
		self.__timeQueue = DoublyLinkedList.DoublyLinkedList()
		self.__keyValueMap: Dict[
			KeyValueKey,
			DoublyLinkedList.DoublyLinkedListNode
		] = dict()

	def _RemoveKeysFromLUT(self, keys: List[KeyValueKey]) -> None:
		for key in keys:
			poppedDllNode = self.__keyValueMap.pop(key, None)
			if poppedDllNode is None:
				self.logger.warning(
					f'Failed to remove key {key} from the lookup table, '
					'key is not found in the lookup table'
				)

	def _InvalidateItem(self, item: KeyValueItem) -> None:
		item.Terminate()
		self._RemoveKeysFromLUT(item.GetKeys())

	def _CleanUpExpiredLockHeld(self, debugLogTimestamp: bool = False) -> None:
		currTimeNS = time.time_ns()

		if debugLogTimestamp:
			oldestExpiredTime = None \
				if self.__timeQueue.empty() \
					else self.__timeQueue.front()[0]

			self.logger.debug(
				f'Current time: {currTimeNS}, '
				f'Oldest expired time: {oldestExpiredTime}'
			)

		while (
			(not self.__timeQueue.empty()) and
			(self.__timeQueue.front()[0] < currTimeNS)
		):
			_, item = self.__timeQueue.popleft()
			self._InvalidateItem(item)

	def CleanUpExpired(self) -> None:
		with self.__storeLock:
			self._CleanUpExpiredLockHeld()

	def __len__(self) -> int:
		with self.__storeLock:
			self._CleanUpExpiredLockHeld()
			return len(self.__timeQueue)

	def NumOfKeys(self) -> int:
		with self.__storeLock:
			self._CleanUpExpiredLockHeld()
			return len(self.__keyValueMap)

	def __contains__(self, key: KeyValueKey) -> bool:
		with self.__storeLock:
			self._CleanUpExpiredLockHeld()
			return key in self.__keyValueMap

	def Put(
		self,
		item: KeyValueItem,
		raiseIfKeyExist: bool = True,
		debugLogTimestamp: bool = False
	) -> None:
		with self.__storeLock:
			self._CleanUpExpiredLockHeld(debugLogTimestamp=debugLogTimestamp)

			currTimeNS = time.time_ns()

			keys = item.GetKeys()
			# Check if the keys are already in the cache
			for key in keys:
				if key in self.__keyValueMap:
					if raiseIfKeyExist:
						raise KeyError(f'Key {key} already exists in cache')
					else:
						# The caller wants us to treat this as an normal case
						return

			expiredTimeNS = currTimeNS + self.__ttlNanoSec
			if debugLogTimestamp:
				self.logger.debug(
					'Adding item - '
					f'Current time: {currTimeNS}, '
					f'Item Key[0]: {keys[0]}, '
					f'Item TTL: {self.__ttlNanoSec}, '
					f'Expired time: {expiredTimeNS}'
				)

			# Add the item to the cache
			dllNode = self.__timeQueue.append([expiredTimeNS, item])
			for key in keys:
				self.__keyValueMap[key] = dllNode

	def Get(
		self,
		key: KeyValueKey,
		default: Union[KeyValueItem, None] = None,
		debugLogTimestamp: bool = False
	) -> Union[KeyValueItem, None]:
		with self.__storeLock:
			self._CleanUpExpiredLockHeld(debugLogTimestamp=debugLogTimestamp)

			currTimeNS = time.time_ns()
			expiredTimeNS = currTimeNS + self.__ttlNanoSec

			if key not in self.__keyValueMap:
				return default
			else:
				# Fetch the DLL node
				dllNode = self.__keyValueMap[key]
				dllNode: DoublyLinkedList.DoublyLinkedListNode

				# Get K-V item
				_, item = dllNode.data
				item: KeyValueItem

				# Update the expired time
				dllNode.data[0] = expiredTimeNS

				# Move the DLL node to the end of the queue
				self.__timeQueue.removeappend(dllNode)

				return item

	def Terminate(self) -> None:
		with self.__storeLock:
			while not self.__timeQueue.empty():
				# Pop the DLL node
				_, item = self.__timeQueue.popleft()

				# Invalidate the item
				self._InvalidateItem(item)

			self.__keyValueMap.clear()

