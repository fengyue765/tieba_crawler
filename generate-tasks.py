import json

# 1. 你的吧列表
bar_list = ['原神内鬼', '有男不玩ml', '半壁江山雪之下', '二游笑话']

# 2. 每个吧要爬的起止页数，可统一设置，也可单独设置
page_start = 1
page_end = 16   # 假如每个吧都爬前16页

# 3. 分割任务的粒度（每个任务多少页，建议小批量，方便分布式）
pages_per_task = 2

# 4. 生成任务
tasks = []
for bar in bar_list:
    cur = page_start
    while cur <= page_end:
        end = min(cur + pages_per_task - 1, page_end)
        task = {
            "bar": bar,
            "page_start": cur,
            "page_end": end
        }
        tasks.append(task)
        cur = end + 1

# 5. 写入tasks.txt
with open("tasks.txt", "w", encoding="utf-8") as f:
    for task in tasks:
        f.write(json.dumps(task, ensure_ascii=False) + "\n")

print(f"已生成 {len(tasks)} 个任务到 tasks.txt")