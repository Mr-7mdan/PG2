import os
from redis import Redis
import json
from datetime import datetime

class VercelKV:
    def __init__(self):
        self.redis = Redis.from_url(os.environ.get('KV_URL'))

    # Cache methods
    async def get(self, key):
        value = self.redis.get(f"cache:{key}")
        return json.loads(value) if value else None

    async def set(self, key, value, timeout=None):
        self.redis.set(f"cache:{key}", json.dumps(value), ex=timeout)

    async def delete(self, key):
        self.redis.delete(f"cache:{key}")

    async def clear(self):
        for key in self.redis.scan_iter("cache:*"):
            self.redis.delete(key)

    # Stats methods
    def get_all_stats(self):
        stats = self.redis.get('stats')
        return json.loads(stats) if stats else {}

    def set_stat(self, key, value):
        stats = self.get_all_stats()
        stats[key] = value
        self.redis.set('stats', json.dumps(stats))

    def clear_stats(self):
        self.redis.delete('stats')

    # Log methods
    async def add_log(self, level, message):
        log_entry = json.dumps({
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        })
        self.redis.lpush('logs', log_entry)

    async def get_logs(self, limit=100, offset=0):
        logs = self.redis.lrange('logs', offset, offset + limit - 1)
        return [json.loads(log) for log in logs]

    async def clear_logs(self):
        self.redis.delete('logs')

    # OMDB cache methods
    async def get_omdb_cache(self, key):
        value = self.redis.get(f"omdb:{key}")
        return json.loads(value) if value else None

    async def set_omdb_cache(self, key, value):
        self.redis.set(f"omdb:{key}", json.dumps(value))

    # Utility methods
    def get_cached_records_count(self):
        return len(list(self.redis.scan_iter("cache:*")))

    def get_logs_count(self):
        return self.redis.llen('logs')

    def get_stats_count(self):
        stats = self.get_all_stats()
        return len(stats)

    # Implement other methods as needed...
