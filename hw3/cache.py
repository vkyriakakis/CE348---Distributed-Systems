import collections

class Cache:
	"""LRU Cache implementation. To be used in HW3."""

	def __init__(self, max_blocks):
		"""Initializes the LRU cache."""

		# Used to trigger an eviction if
		# the cache becomes full
		self._max_blocks = max_blocks
		self._block_num = 0

		# Dictionary indexed by some user given key
		# used for fast lookup of block contents
		self._block_data = {}

		# Queue used to store LRU recency information (instead
		# of an age counter for each block). The first element
		# in the queue is the most recent, the last is the least recent
		self._recency_queue = collections.deque()

	def insert(self, key, data):
		"""Inserts the block <key> with contents <data> into the cache.
		If the cache was full, the least recently used 
		block is evicted to make space."""

		# Check if the block exists in the cache
		# if so, just update the data
		if key in self._block_data:
			self._block_data[key] = data
			# Don't make it an LRU, a lookup preceded the update
			return
		elif self._max_blocks == 0:
			# If no blocks do nothing
			return
		elif self._block_num == self._max_blocks:
			# Evict the LRU block (end of queue)
			evict_key = self._recency_queue.pop()
			del self._block_data[evict_key]
		else:
			self._block_num += 1

		# The inserted block becomes the most recently used
		self._block_data[key] = data
		self._recency_queue.appendleft(key)

	def lookup(self, key):
		"""Checks if the block <key> is in the cache, if yes it returns the
		contents of it, else it returns None. Makes <key> the most recent block."""
		if key in self._block_data:
			# If found in the cache, that's a hit, so make it
			# the most recently used block
			self._recency_queue.remove(key)
			self._recency_queue.appendleft(key)
			return self._block_data[key]

		return None

	def remove(self, key):
		"""Removes all traces of a block from the cache."""
		if key in self._block_data:
			del self._block_data[key]
			self._recency_queue.remove(key)
			self._block_num -= 1