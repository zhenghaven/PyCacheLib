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


class TTL(Terminable):

	def __init__(self, ttl: float) -> None:
		super(TTL, self).__init__()

		self.logger = logging.getLogger(
			f'{__name__}.{self.__class__.__name__}'
		)

		self.ttl = ttl

	def CleanUpExpired(self) -> None:
		'''
		### CleanUpExpired - Clean up expired items
		'''
		raise NotImplementedError('Not implemented')

