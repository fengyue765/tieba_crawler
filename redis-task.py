import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
TASKS_REDIS_KEY = "tieba_tasks"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
r.delete(TASKS_REDIS_KEY)

with open("tasks.txt", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            r.rpush(TASKS_REDIS_KEY, line)

print("已导入全部任务到Redis队列")