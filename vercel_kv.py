import os
from redis import Redis
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class VercelKV:
    def __init__(self):
        kv_url = os.environ.get('KV_URL')
        if not kv_url:
            raise ValueError("KV_URL environment variable is not set")
        self.redis = Redis.from_url(kv_url, socket_timeout=5, socket_connect_timeout=5, retry_on_timeout=True)
        self.fallback_storage = {}  # In-memory fallback storage

    def _safe_redis_operation(self, operation, fallback_operation, default_value=None):
        try:
            return operation()
        except Exception as e:
            logger.error(f"Redis operation failed: {str(e)}")
            try:
                return fallback_operation()
            except Exception as fe:
                logger.error(f"Fallback operation failed: {str(fe)}")
                return default_value

    # Cache methods
    def get(self, key):
        return self._safe_redis_operation(
            lambda: json.loads(self.redis.get(f"cache:{key}") or 'null'),
            lambda: self.fallback_storage.get(f"cache:{key}"),
            None
        )

    def set(self, key, value, timeout=None):
        self._safe_redis_operation(
            lambda: self.redis.set(f"cache:{key}", json.dumps(value), ex=timeout),
            lambda: self.fallback_storage.update({f"cache:{key}": value})
        )

    def delete(self, key):
        self._safe_redis_operation(
            lambda: self.redis.delete(f"cache:{key}"),
            lambda: self.fallback_storage.pop(f"cache:{key}", None)
        )

    def clear(self):
        self._safe_redis_operation(
            lambda: [self.redis.delete(key) for key in self.redis.scan_iter("cache:*")],
            lambda: self.fallback_storage.clear()
        )

    # Stats methods
    def get_all_stats(self):
        return self._safe_redis_operation(
            lambda: json.loads(self.redis.get('stats') or '{}'),
            lambda: self.fallback_storage.get('stats', {}),
            {}
        )

    def set_stat(self, key, value):
        stats = self.get_all_stats()
        stats[key] = value
        self._safe_redis_operation(
            lambda: self.redis.set('stats', json.dumps(stats)),
            lambda: self.fallback_storage.update({'stats': stats})
        )

    def clear_stats(self):
        self._safe_redis_operation(
            lambda: self.redis.delete('stats'),
            lambda: self.fallback_storage.pop('stats', None)
        )

    # Log methods
    def add_log(self, level, message):
        log_entry = json.dumps({
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        })
        self._safe_redis_operation(
            lambda: self.redis.lpush('logs', log_entry),
            lambda: self.fallback_storage.setdefault('logs', []).append(log_entry)
        )

    def get_logs(self, limit=100, offset=0):
        return self._safe_redis_operation(
            lambda: [json.loads(log) for log in self.redis.lrange('logs', offset, offset + limit - 1)],
            lambda: [json.loads(log) for log in self.fallback_storage.get('logs', [])[offset:offset+limit]],
            []
        )

    def clear_logs(self):
        self._safe_redis_operation(
            lambda: self.redis.delete('logs'),
            lambda: self.fallback_storage.pop('logs', None)
        )

    # OMDB cache methods
    def get_omdb_cache(self, key):
        return self._safe_redis_operation(
            lambda: json.loads(self.redis.get(f"omdb:{key}") or 'null'),
            lambda: self.fallback_storage.get(f"omdb:{key}"),
            None
        )

    def set_omdb_cache(self, key, value):
        self._safe_redis_operation(
            lambda: self.redis.set(f"omdb:{key}", json.dumps(value)),
            lambda: self.fallback_storage.update({f"omdb:{key}": value})
        )

    # Utility methods
    def get_cached_records_count(self):
        return self._safe_redis_operation(
            lambda: self.redis.dbsize(),
            lambda: len([k for k in self.fallback_storage.keys() if k.startswith('cache:')]),
            0
        )

    def get_logs_count(self):
        return self._safe_redis_operation(
            lambda: self.redis.llen('logs'),
            lambda: len(self.fallback_storage.get('logs', [])),
            0
        )

    def get_stats_count(self):
        stats = self.get_all_stats()
        return len(stats)

    # Implement other methods as needed...
