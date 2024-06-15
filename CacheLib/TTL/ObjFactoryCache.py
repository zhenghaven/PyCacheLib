#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Copyright (c) 2024 Haofan Zheng
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
###


import time

from collections import deque

from .. import LockwSLD
from .Interfaces import ClassWideTTL, Terminable


class ObjFactoryCache(ClassWideTTL):
	'''
	## ObjFactoryCache - Object Factory Cache

	This cache is designed to store instances of a class that are constructed
	using the exact same parameters.
	The callers are expected to get 1 instance at a time, use it, and return it
	to this cache.
	Instances that have not been retrieved for `TTL` seconds will be removed.
	When there is no instance available in the cache, it will create a new
	instance.
	'''

	DEFAULT_TRACK_INST_IN_USE = True
	'''
	Wether or not to track (by keeping a reference) the instances that are
	in use by the caller (i.e., fetched by the caller).
	By tracking the instances, we can attempt to call `Terminate` method on the
	instance to clean up resources during terminating in a multi-threaded
	scenario.
	'''

	IDLE_ITEM_CONTAINER_CLASS = deque
	'''
	The container class used to store idle items.

	The default is `collections.deque`, since we only works on both ends of the
	container (append and pop), so `deque` should provide better performance.
	'''

	DEFAULT_ITEM_BASE_CLASS = Terminable

	def __init__(
		self,
		ttl: float = 3600.0,
		objCls: type = DEFAULT_ITEM_BASE_CLASS,
		objArgs: tuple = tuple(),
		objKwargs: dict = dict(),
	) -> None:
		'''
		### Constructor

		- Parameters:
			- ttl: Time to live for each object in seconds
			- objCls: Class to create objects from
			- objArgs: Arguments to pass to the class constructor
			- objKwargs: Keyword arguments to pass to the class constructor
		'''
		super(ObjFactoryCache, self).__init__(ttl=ttl)

		self.objCls = objCls
		self.objArgs = objArgs
		self.objKwargs = objKwargs

		self.objLock = LockwSLD.LockwSLD()
		self.idleObjs = self.IDLE_ITEM_CONTAINER_CLASS()
		self.inUseObjs = {}

	def CleanUpExpiredLocked(self) -> None:

		currTime = time.time()
		while (
			(len(self.idleObjs) > 0) and # at least 1 item in the queue and
			((self.idleObjs[0][0] + self.ttl) < currTime) # the oldest item is expired
		):
			# remove expired objects
			_, obj = self.idleObjs.popleft()
			obj.Terminate()

	def CleanUpExpired(self) -> None:
		with self.objLock:
			self.CleanUpExpiredLocked()

	def _PopIdleObjForUseLocked(self) -> Terminable:
		'''
		The position of the item being popped from the idle queue affects the
		outcome of the overall cache performance.

		- If we pop from the left (the oldest item), the cache will be more
		  lenient to keep as much objects as possible, since it keeps refreshing
		  the oldest item.
		  This strategy consumes more memory, but it is more tolerant to sudden
		  burst of requests.
		- If we pop from the right (the newest item), the cache will be more
		  aggressive to remove a number of objects that are not needed for
		  a while, since it keeps refreshing newer items and let the older
		  no-needed items expire.
		  This strategy consumes less memory, but it is less tolerant to sudden
		  burst of requests.
		- A potential balanced strategy would be using a random position to pop
		  the item.

		Here we pop from the right (the newest item).

		Subclasses can override this method to change the popping strategy.
		'''
		_, obj = self.idleObjs.pop()
		return obj

	def Get(self, trackInUse: bool = DEFAULT_TRACK_INST_IN_USE) -> Terminable:
		with self.objLock:
			self.CleanUpExpiredLocked()

			if len(self.idleObjs) == 0:
				# create a new object
				obj: Terminable = self.objCls(*self.objArgs, **self.objKwargs)
			else:
				obj: Terminable = self._PopIdleObjForUseLocked()

			if trackInUse:
				self.inUseObjs[obj.IDInt] = obj

			return obj

	def Put(self, obj: Terminable) -> None:
		with self.objLock:
			self.CleanUpExpiredLocked()

			if obj.IDInt in self.inUseObjs:
				del self.inUseObjs[obj.IDInt]

			# the newest item is always appended to the right
			self.idleObjs.append((time.time(), obj))

	def Untrack(self, obj: Terminable) -> None:
		with self.objLock:
			self.CleanUpExpiredLocked()

			if obj.IDInt in self.inUseObjs:
				del self.inUseObjs[obj.IDInt]

	def Terminate(self) -> None:
		with self.objLock:
			while len(self.idleObjs) > 0:
				_, obj = self.idleObjs.popleft()
				obj.Terminate()
			for _, obj in self.inUseObjs.items():
				obj.Terminate()
			self.inUseObjs = {}

	def __len__(self) -> int:
		with self.objLock:
			self.CleanUpExpiredLocked()
			return len(self.idleObjs) + len(self.inUseObjs)

	def NumberOfIdle(self) -> int:
		with self.objLock:
			self.CleanUpExpiredLocked()
			return len(self.idleObjs)

