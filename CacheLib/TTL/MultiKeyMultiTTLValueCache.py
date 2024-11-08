#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


import time

from typing import Union

from sortedcontainers import SortedDict

from .. import LockwSLD
from .Interfaces import KeyValueKey, KeyValueItem, TTL


class MultiKeyMultiTTLValueCache(TTL):

	def __init__(self,) -> None:
		super(MultiKeyMultiTTLValueCache, self).__init__()

		self.__storeLock = LockwSLD.LockwSLD()
		self.__timeQueue = SortedDict()
		self.__keyValueMap = dict()

	def __RemoveKeysFromLUT(self, keys: list[KeyValueKey]) -> None:
		for key in keys:
			poppedItem = self.__keyValueMap.pop(key, None)
			if poppedItem is None:
				self.logger.warning(
					f'Failed to remove key {key} from the lookup table'
				)

	def __InvalidateItem(self, item: KeyValueItem) -> None:
		item.Terminate()
		self.__RemoveKeysFromLUT(item.GetKeys())

	def CleanUpExpiredLocked(self, debugLogTimestamp: bool = False) -> None:
		currTimeNS = time.time_ns()

		if debugLogTimestamp:
			oldestExpiredTime = None \
				if (not self.__timeQueue) \
					else self.__timeQueue.peekitem(0)[0]
			self.logger.debug(
				f'Current time: {currTimeNS}, '
				f'Oldest expired time: {oldestExpiredTime}'
			)

		while (
			(len(self.__timeQueue) > 0) and
			(self.__timeQueue.peekitem(0)[0] < currTimeNS)
		):
			_, item = self.__timeQueue.popitem(0)
			self.__InvalidateItem(item)

	def CleanUpExpired(self) -> None:
		with self.__storeLock:
			self.CleanUpExpiredLocked()

	def __len__(self) -> int:
		with self.__storeLock:
			self.CleanUpExpiredLocked()
			return len(self.__timeQueue)

	def NumOfKeys(self) -> int:
		with self.__storeLock:
			self.CleanUpExpiredLocked()
			return len(self.__keyValueMap)

	def __contains__(self, key: KeyValueKey) -> bool:
		with self.__storeLock:
			self.CleanUpExpiredLocked()
			return key in self.__keyValueMap

	def Put(
		self,
		item: KeyValueItem,
		raiseIfKeyExist: bool = True,
		debugLogTimestamp: bool = False,
	) -> None:
		with self.__storeLock:
			self.CleanUpExpiredLocked(debugLogTimestamp=debugLogTimestamp)

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

			expiredTimeNS = currTimeNS + item.GetTTLNanoSec()
			if debugLogTimestamp:
				self.logger.debug(
					f'Current time: {currTimeNS}, '
					f'Item Key[0]: {keys[0]}, '
					f'Item TTL: {item.GetTTLNanoSec()}, '
					f'Expired time: {expiredTimeNS}'
				)

			# Add the item to the cache
			for key in keys:
				self.__keyValueMap[key] = item
			self.__timeQueue[expiredTimeNS] = item

	def Get(
		self,
		key: KeyValueKey,
		default: Union[KeyValueItem, None] = None,
		debugLogTimestamp: bool = False,
	) -> Union[KeyValueItem, None]:
		with self.__storeLock:
			self.CleanUpExpiredLocked(debugLogTimestamp=debugLogTimestamp)

			return self.__keyValueMap.get(key, default)

	def Terminate(self) -> None:
		with self.__storeLock:
			for _, item in self.__timeQueue.items():
				self.__InvalidateItem(item)
			self.__timeQueue.clear()
			self.__keyValueMap.clear()

