import os
from redis import Redis
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class VercelKV:
    def __init__(self):
        self.fallback_storage = {}  # In-memory fallback storage
        kv_url = os.environ.get('KV_URL')
        if kv_url:
            try:
                self.redis = Redis.from_url(kv_url, socket_timeout=10, socket_connect_timeout=10, retry_on_timeout=True, max_connections=10)
                self.redis.ping()  # Test the connection
                logger.info("Successfully connected to Redis")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {str(e)}. Using fallback storage.")
                self.redis = None
        else:
            logger.warning("KV_URL not set. Using fallback storage.")
            self.redis = None

    def _safe_operation(self, redis_op, fallback_op):
        if self.redis:
            try:
                return redis_op()
            except Exception as e:
                logger.error(f"Redis operation failed: {str(e)}. Using fallback storage.")
                self.redis = None  # Disable Redis for future operations
        return fallback_op()

    def _safe_json_dumps(self, data):
        try:
            return json.dumps(data, ensure_ascii=False)
        except Exception as e:
            logger.error(f"JSON encoding error: {str(e)}")
            return json.dumps(str(data))

    def _safe_json_loads(self, data):
        try:
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"JSON decoding error: {str(e)}")
            return None

    # Cache methods
    def get(self, key):
        return self._safe_operation(
            lambda: self._safe_json_loads(self.redis.get(f"cache:{key}")),
            lambda: self.fallback_storage.get(f"cache:{key}")
        )

    def set(self, key, value, timeout=None):
        json_value = self._safe_json_dumps(value)
        self._safe_operation(
            lambda: self.redis.set(f"cache:{key}", json_value, ex=timeout),
            lambda: self.fallback_storage.update({f"cache:{key}": value})
        )

    def delete(self, key):
        self._safe_operation(
            lambda: self.redis.delete(f"cache:{key}"),
            lambda: self.fallback_storage.pop(f"cache:{key}", None)
        )

    def clear(self):
        self._safe_operation(
            lambda: [self.redis.delete(key) for key in self.redis.scan_iter("cache:*")],
            lambda: self.fallback_storage.clear()
        )

    # Stats methods
    def get_all_stats(self):
        return self._safe_operation(
            lambda: self._safe_json_loads(self.redis.get('stats')) or {},
            lambda: self.fallback_storage.get('stats', {})
        )

    def set_stat(self, key, value):
        stats = self.get_all_stats()
        stats[key] = value
        json_stats = self._safe_json_dumps(stats)
        self._safe_operation(
            lambda: self.redis.set('stats', json_stats),
            lambda: self.fallback_storage.update({'stats': stats})
        )

    def clear_stats(self):
        self._safe_operation(
            lambda: self.redis.delete('stats'),
            lambda: self.fallback_storage.pop('stats', None)
        )

    # Log methods
    def add_log(self, level, message):
        log_entry = self._safe_json_dumps({
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        })
        result = self._safe_operation(
            lambda: self.redis.lpush('logs', log_entry),
            lambda: self.fallback_storage.setdefault('logs', []).insert(0, log_entry)
        )
        logger.info(f"Log added: {log_entry}")
        return result

    def get_logs(self, limit=100, offset=0):
        logs = self._safe_operation(
            lambda: [self._safe_json_loads(log) for log in self.redis.lrange('logs', offset, offset + limit - 1)],
            lambda: [self._safe_json_loads(log) for log in self.fallback_storage.get('logs', [])[offset:offset+limit]]
        )
        logger.info(f"Retrieved {len(logs)} logs")
        return logs

    def clear_logs(self):
        self._safe_operation(
            lambda: self.redis.delete('logs'),
            lambda: self.fallback_storage.pop('logs', None)
        )

    # OMDB cache methods
    def get_omdb_cache(self, key):
        return self._safe_operation(
            lambda: json.loads(self.redis.get(f"omdb:{key}") or 'null'),
            lambda: self.fallback_storage.get(f"omdb:{key}")
        )

    def set_omdb_cache(self, key, value):
        self._safe_operation(
            lambda: self.redis.set(f"omdb:{key}", json.dumps(value)),
            lambda: self.fallback_storage.update({f"omdb:{key}": value})
        )

    # Utility methods
    def get_cached_records_count(self):
        return self._safe_operation(
            lambda: self.redis.dbsize(),
            lambda: len([k for k in self.fallback_storage.keys() if k.startswith('cache:')])
        )

    def get_logs_count(self):
        return self._safe_operation(
            lambda: self.redis.llen('logs'),
            lambda: len(self.fallback_storage.get('logs', []))
        )

    def get_stats_count(self):
        stats = self.get_all_stats()
        return len(stats)

    # Implement other methods as needed...
