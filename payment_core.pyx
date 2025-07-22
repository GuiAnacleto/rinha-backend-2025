# payment_core.pyx - Implementação Cython simplificada
cimport cython

cdef class FastMemoryDB:
    cdef dict _data
    cdef set _processed
    cdef object _lock

    def __init__(self):
        self._data = {"default": [], "fallback": []}
        self._processed = set()
        import asyncio
        self._lock = asyncio.Lock()

    async def add_payment(self, str correlation_id, double amount, double timestamp, str service):
        async with self._lock:
            if correlation_id in self._processed:
                return False
            self._processed.add(correlation_id)
            self._data[service].append((timestamp, amount))
            return True

    async def get_summary_fast(self, double from_ts, double to_ts, str service):
        cdef double total = 0.0
        cdef int count = 0
        cdef double ts, amt

        async with self._lock:
            for ts, amt in self._data[service]:
                if from_ts <= ts <= to_ts:
                    total += amt
                    count += 1

            return {"totalRequests": count, "totalAmount": total}

    async def clear(self):
        async with self._lock:
            self._processed.clear()
            self._data["default"].clear()
            self._data["fallback"].clear()