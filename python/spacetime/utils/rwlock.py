#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copied and modified from readerwriterlock pip package.
"""Read Write Lock."""

import threading
import time

from typing import Callable
from typing import Optional
from typing import Type
from types import TracebackType


class RWLockRead():
	"""A Read/Write lock giving preference to Reader."""

	def __init__(self, lock_factory: Callable[[], threading.Lock] = threading.Lock) -> None:
		"""Init."""
		self.v_read_count = 0
		self.c_resource = lock_factory()
		self.c_lock_read_count = lock_factory()

	class _aReader():
		def __init__(self, p_RWLock: "RWLockRead") -> None:
			self.c_rw_lock = p_RWLock
			self.v_locked = False

		def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
			"""Acquire a lock."""
			p_timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
			c_deadline = None if p_timeout is None else (time.time() + p_timeout)
			if not self.c_rw_lock.c_lock_read_count.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				raise RuntimeError("Waiting too long to get count lock")
				return False
			self.c_rw_lock.v_read_count += 1
			if 1 == self.c_rw_lock.v_read_count:
				if not self.c_rw_lock.c_resource.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
					self.c_rw_lock.v_read_count -= 1
					self.c_rw_lock.c_lock_read_count.release()
					raise RuntimeError("Waiting too long to get main lock")
					return False
			self.c_rw_lock.c_lock_read_count.release()
			self.v_locked = True
			return self

		def release(self) -> None:
			"""Release the lock."""
			if not self.v_locked: raise RuntimeError("cannot release un-acquired lock")
			self.v_locked = False
			self.c_rw_lock.c_lock_read_count.acquire()
			self.c_rw_lock.v_read_count -= 1
			if 0 == self.c_rw_lock.v_read_count:
				self.c_rw_lock.c_resource.release()
			self.c_rw_lock.c_lock_read_count.release()

		def locked(self) -> bool:
			"""Answer to 'is it currently locked?'."""
			return self.v_locked

		def __enter__(self) -> None:
			self.acquire()

		def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
			self.release()
			return False

	class _aWriter():
		def __init__(self, p_RWLock: "RWLockRead") -> None:
			self.c_rw_lock = p_RWLock
			self.v_locked = False

		def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
			"""Acquire a lock."""
			self.v_locked = self.c_rw_lock.c_resource.acquire(blocking, timeout)
			return self

		def release(self) -> None:
			"""Release the lock."""
			if not self.v_locked: raise RuntimeError("cannot release un-acquired lock")
			self.v_locked = False
			self.c_rw_lock.c_resource.release()

		def locked(self) -> bool:
			"""Answer to 'is it currently locked?'."""
			return self.v_locked

		def __enter__(self) -> None:
			self.acquire()

		def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
			self.release()
			return False

	def gen_rlock(self) -> "RWLockRead._aReader":
		"""Generate a reader lock."""
		return RWLockRead._aReader(self)

	def gen_wlock(self) -> "RWLockRead._aWriter":
		"""Generate a writer lock."""
		return RWLockRead._aWriter(self)


class RWLockWrite():
	"""A Read/Write lock giving preference to Writer."""

	def __init__(self, lock_factory: Callable[[], threading.Lock] = threading.Lock) -> None:
		"""Init."""
		self.v_read_count = 0
		self.v_write_count = 0
		self.c_lock_read_count = lock_factory()
		self.c_lock_write_count = lock_factory()
		self.c_lock_read_entry = lock_factory()
		self.c_lock_read_try = lock_factory()
		self.c_resource = lock_factory()

	class _aReader():
		def __init__(self, p_RWLock: "RWLockWrite") -> None:
			self.c_rw_lock = p_RWLock
			self.v_locked = False

		def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
			"""Acquire a lock."""
			p_timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
			c_deadline = None if p_timeout is None else (time.time() + p_timeout)
			if not self.c_rw_lock.c_lock_read_entry.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				raise RuntimeError("Waiting too long to get count lock")
				return False
			if not self.c_rw_lock.c_lock_read_try.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				self.c_rw_lock.c_lock_read_entry.release()
				return False
			if not self.c_rw_lock.c_lock_read_count.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				self.c_rw_lock.c_lock_read_try.release()
				self.c_rw_lock.c_lock_read_entry.release()
				return False
			self.c_rw_lock.v_read_count += 1
			if 1 == self.c_rw_lock.v_read_count:
				if not self.c_rw_lock.c_resource.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
					self.c_rw_lock.c_lock_read_try.release()
					self.c_rw_lock.c_lock_read_entry.release()
					self.c_rw_lock.v_read_count -= 1
					self.c_rw_lock.c_lock_read_count.release()
					raise RuntimeError("Waiting too long to get read lock")
					return False
			self.c_rw_lock.c_lock_read_count.release()
			self.c_rw_lock.c_lock_read_try.release()
			self.c_rw_lock.c_lock_read_entry.release()
			self.v_locked = True
			return self

		def release(self) -> None:
			"""Release the lock."""
			if not self.v_locked: raise RuntimeError("cannot release un-acquired lock")
			self.v_locked = False
			self.c_rw_lock.c_lock_read_count.acquire()
			self.c_rw_lock.v_read_count -= 1
			if 0 == self.c_rw_lock.v_read_count:
				self.c_rw_lock.c_resource.release()
			self.c_rw_lock.c_lock_read_count.release()

		def locked(self) -> bool:
			"""Answer to 'is it currently locked?'."""
			return self.v_locked

		def __enter__(self) -> None:
			self.acquire()

		def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
			self.release()
			return False

	class _aWriter():
		def __init__(self, p_RWLock: "RWLockWrite") -> None:
			self.c_rw_lock = p_RWLock
			self.v_locked = False

		def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
			"""Acquire a lock."""
			p_timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
			c_deadline = None if p_timeout is None else (time.time() + p_timeout)
			if not self.c_rw_lock.c_lock_write_count.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				return False
			self.c_rw_lock.v_write_count += 1
			if 1 == self.c_rw_lock.v_write_count:
				if not self.c_rw_lock.c_lock_read_try.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
					self.c_rw_lock.v_write_count -= 1
					self.c_rw_lock.c_lock_write_count.release()
					return False
			self.c_rw_lock.c_lock_write_count.release()
			if not self.c_rw_lock.c_resource.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				self.c_rw_lock.c_lock_write_count.acquire()
				self.c_rw_lock.v_write_count -= 1
				if 0 == self.c_rw_lock.v_write_count:
					self.c_rw_lock.c_lock_read_try.release()
				self.c_rw_lock.c_lock_write_count.release()
				return False
			self.v_locked = True
			return self

		def release(self) -> None:
			"""Release the lock."""
			if not self.v_locked: raise RuntimeError("cannot release un-acquired lock")
			self.v_locked = False
			self.c_rw_lock.c_resource.release()
			self.c_rw_lock.c_lock_write_count.acquire()
			self.c_rw_lock.v_write_count -= 1
			if 0 == self.c_rw_lock.v_write_count:
				self.c_rw_lock.c_lock_read_try.release()
			self.c_rw_lock.c_lock_write_count.release()

		def locked(self) -> bool:
			"""Answer to 'is it currently locked?'."""
			return self.v_locked

		def __enter__(self) -> None:
			self.acquire()

		def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
			self.release()
			return False

	def gen_rlock(self) -> "RWLockWrite._aReader":
		"""Generate a reader lock."""
		return RWLockWrite._aReader(self)

	def gen_wlock(self) -> "RWLockWrite._aWriter":
		"""Generate a writer lock."""
		return RWLockWrite._aWriter(self)


class RWLockFair():
	"""A Read/Write lock giving fairness to both Reader and Writer."""

	def __init__(self, lock_factory: Callable[[], threading.Lock] = threading.Lock) -> None:
		"""Init."""
		self.v_read_count = 0
		self.c_lock_read_count = lock_factory()
		self.c_lock_read = lock_factory()
		self.c_lock_write = lock_factory()

	class _aReader():
		def __init__(self, p_RWLock: "RWLockFair") -> None:
			self.c_rw_lock = p_RWLock
			self.v_locked = False

		def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
			"""Acquire a lock."""
			p_timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
			c_deadline = None if p_timeout is None else (time.time() + p_timeout)
			if not self.c_rw_lock.c_lock_read.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				raise RuntimeError("Waiting too long to get count lock")
				return False
			if not self.c_rw_lock.c_lock_read_count.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				self.c_rw_lock.c_lock_read.release()
				raise RuntimeError("Waiting too long to get fair read lock")
				return False
			self.c_rw_lock.v_read_count += 1
			if 1 == self.c_rw_lock.v_read_count:
				if not self.c_rw_lock.c_lock_write.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
					self.c_rw_lock.v_read_count -= 1
					self.c_rw_lock.c_lock_read_count.release()
					self.c_rw_lock.c_lock_read.release()
					return False
			self.c_rw_lock.c_lock_read_count.release()
			self.c_rw_lock.c_lock_read.release()
			self.v_locked = True
			return self

		def release(self) -> None:
			"""Release the lock."""
			if not self.v_locked: raise RuntimeError("cannot release un-acquired lock")
			self.v_locked = False
			self.c_rw_lock.c_lock_read_count.acquire()
			self.c_rw_lock.v_read_count -= 1
			if 0 == self.c_rw_lock.v_read_count:
				self.c_rw_lock.c_lock_write.release()
			self.c_rw_lock.c_lock_read_count.release()

		def locked(self) -> bool:
			"""Answer to 'is it currently locked?'."""
			return self.v_locked

		def __enter__(self) -> None:
			self.acquire()

		def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
			self.release()
			return False

	class _aWriter():
		def __init__(self, p_RWLock: "RWLockFair") -> None:
			self.c_rw_lock = p_RWLock
			self.v_locked = False

		def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
			"""Acquire a lock."""
			p_timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
			c_deadline = None if p_timeout is None else (time.time() + p_timeout)
			if not self.c_rw_lock.c_lock_read.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				return False
			if not self.c_rw_lock.c_lock_write.acquire(blocking=True, timeout=-1 if c_deadline is None else max(0, c_deadline - time.time())):
				self.c_rw_lock.c_lock_read.release()
				return False
			self.v_locked = True
			return self

		def release(self) -> None:
			"""Release the lock."""
			if not self.v_locked: raise RuntimeError("cannot release un-acquired lock")
			self.v_locked = False
			self.c_rw_lock.c_lock_write.release()
			self.c_rw_lock.c_lock_read.release()

		def locked(self) -> bool:
			"""Answer to 'is it currently locked?'."""
			return self.v_locked

		def __enter__(self) -> None:
			self.acquire()

		def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
			self.release()
			return False

	def gen_rlock(self) -> "RWLockFair._aReader":
		"""Generate a reader lock."""
		return RWLockFair._aReader(self)

	def gen_wlock(self) -> "RWLockFair._aWriter":
		"""Generate a writer lock."""
		return RWLockFair._aWriter(self)
