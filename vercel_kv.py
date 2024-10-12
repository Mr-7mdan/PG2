import os
from redis import Redis
import json
from datetime import datetime

class VercelKV:
    def __init__(self):
        kv_url = os.environ.get('KV_URL')
        if not kv_url:
            raise ValueError("KV_URL environment variable is not set")
        self.redis = Redis.from_url(kv_url, socket_timeout=5)

    def _safe_redis_operation(self, operation):
        try:
            return operation()
        except Exception as e:
            print(f"Redis operation failed: {str(e)}")
            return None

    # Cache methods
    def get(self, key):
        return self._safe_redis_operation(lambda: json.loads(self.redis.get(f"cache:{key}") or 'null'))

    def set(self, key, value, timeout=None):
        self._safe_redis_operation(lambda: self.redis.set(f"cache:{key}", json.dumps(value), ex=timeout))

    def delete(self, key):
        self._safe_redis_operation(lambda: self.redis.delete(f"cache:{key}"))

    def clear(self):
        self._safe_redis_operation(lambda: [self.redis.delete(key) for key in self.redis.scan_iter("cache:*")])

    # Stats methods
    def get_all_stats(self):
        return self._safe_redis_operation(lambda: json.loads(self.redis.get('stats') or '{}'))

    def set_stat(self, key, value):
        stats = self.get_all_stats()
        stats[key] = value
        self._safe_redis_operation(lambda: self.redis.set('stats', json.dumps(stats)))

    def clear_stats(self):
        self._safe_redis_operation(lambda: self.redis.delete('stats'))

    # Log methods
    def add_log(self, level, message):
        log_entry = json.dumps({
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        })
        self._safe_redis_operation(lambda: self.redis.lpush('logs', log_entry))

    def get_logs(self, limit=100, offset=0):
        return self._safe_redis_operation(lambda: [json.loads(log) for log in self.redis.lrange('logs', offset, offset + limit - 1)])

    def clear_logs(self):
        self._safe_redis_operation(lambda: self.redis.delete('logs'))

    # OMDB cache methods
    def get_omdb_cache(self, key):
        return self._safe_redis_operation(lambda: json.loads(self.redis.get(f"omdb:{key}") or 'null'))

    def set_omdb_cache(self, key, value):
        self._safe_redis_operation(lambda: self.redis.set(f"omdb:{key}", json.dumps(value)))

    # Utility methods
    def get_cached_records_count(self):
        return self._safe_redis_operation(lambda: self.redis.dbsize()) or 0

    def get_logs_count(self):
        return self._safe_redis_operation(lambda: self.redis.llen('logs')) or 0

    def get_stats_count(self):
        stats = self.get_all_stats()
        return len(stats)

    # Implement other methods as needed...
