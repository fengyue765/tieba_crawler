# 分布式贴吧爬虫项目（Redis任务池版）使用说明

本项目支持**多进程/多机分布式采集百度贴吧**，任务分发基于Redis队列，具备断点续爬、人工验证码支持、Cookie/代理池、任意扩缩容等能力。

---

## 1. 环境准备

### 1.1 Python依赖安装

支持Python 3.7+  
建议Anaconda/venv环境，命令行运行：

```bash
pip install selenium beautifulsoup4 redis
```

### 1.2 Edge浏览器驱动

- 下载与你Edge浏览器版本一致的 [msedgedriver.exe](https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/)。
- 将其放到代码指定路径，并修改 `EDGE_DRIVER_PATH`。

### 1.3 Redis服务安装

由于该程序使用的是Microsoft Edge与EdgeDriver，故建议在windows系统中运行：

- 下载[官方Windows Redis社区版](https://github.com/microsoftarchive/redis/releases) 或 [Memurai](https://www.memurai.com/)。
- 解压，**用管理员权限**运行 `redis-server.exe`，或双击直接启动。
- 默认监听 `localhost:6379`。

---

## 2. 项目结构

```
├── tieba-spidering.py            # 主爬虫程序 
├── redis-task.py                 # 任务池初始化脚本  
├── generate-tasks.py             # 用于根据任务需求生成tasks.txt文件  
├── tasks.txt                     # 任务源文件（每行一个JSON任务），手动生成  
├── cookies.txt                   # Cookie池（每行一个完整cookie字符串），手动创建  
├── proxies.txt                   # 代理池（每行一个代理IP:端口），手动创建  
├── resume_info.json              # 断点续爬信息，自动生成  
├── tieba_crawler.log             # 运行日志，自动生成  
├── tasks_inprogress.txt          # 采集中任务记录，自动生成  
├── tasks_done.txt                # 完成任务记录，自动生成  
└── output/                       # 采集结果保存目录
```

---

## 3. 使用流程

### 3.1 准备任务文件

编辑generate-tasks.py中需要爬取的**吧列表、起止页数**与分割粒度。运行该脚本批量生成tasks.txt任务文件：

```bash
python generate-tasks.py
```

### 3.2 导入任务到 Redis

每次任务变更后，**只需一次**：

```bash
redis-task.py 
```

> 此脚本会清空原有任务队列并重新导入。

### 3.3 配置Cookie池与代理池

在环境目录创建以下两个文件：

- `cookies.txt`：每行一个完整百度贴吧cookie（建议多账号，人工获取，确保有效）。
- `proxies.txt`：每行一个“ip:port”，可为空（无代理时程序会本地直连）。

### 3.4 配置Edge驱动路径

编辑主程序（`tieba-spidering.py `）顶部的 `EDGE_DRIVER_PATH`。

### 3.5 启动Redis服务

确保Redis服务器已启动，且`REDIS_HOST`、`REDIS_PORT`与主程序中的设置一致。

### 3.6 启动爬虫

打开多个命令行终端（或多台机器/多容器），**可并发启动多个进程**：

```bash
python tieba-spidering.py
```

每个进程会自动从Redis领取任务，互不冲突。

---

## 4. 断点续爬说明

- 任务级：每个任务领取后记录至 `tasks_inprogress.txt`，完成后写入 `tasks_done.txt`，程序异常退出可自动恢复。
- 帖子/页码级：主程序会在 `resume_info.json` 记录当前正在采集的吧、页码、帖子，异常断开后自动续爬。

---

## 5. 验证码/风控处理

- 遇到百度风控（滑块/验证码），程序会弹出可交互浏览器，**等待人工操作**。
- 人工操作后按提示回车，爬虫自动继续，无需手工重启。
- 验证码超时/未解决，则自动切换代理/Cookie。

> 我自己实际运行时，每隔约15min每个进程就需要人工操作一次。若不操作，则必定会进入风控冷却。可能是由于cookie或者代理池已经污染导致，不过懒得改了，能跑就行。

---

## 6. 采集结果查看

- 采集的帖子内容保存在 `output/吧名/帖子标题.txt`。
- 日志见 `tieba_crawler.log`。

---

## 7. 常见问题与排查

### 7.1 多进程任务重复领取？

- 确保所有进程/机器**连接同一个Redis服务器**，且init_redis_tasks只运行一次。
- 查看日志，确认`lpop`领取机制生效。需要注意的是，lpop会把未执行的分片任务弹出。

### 7.2 Redis无法启动或连接不上？

- 检查防火墙、端口占用、管理员权限。
- 用`redis-cli ping`或`import redis; redis.Redis().ping()`测试网络连通性。

### 7.3 AttributeError: module 'redis' has no attribute 'Redis'？

- 检查代码目录下有无`redis.py`文件，若有请重命名并删除`redis.pyc`/`__pycache__`。

### 7.4 Edge驱动报错？

- Edge浏览器版本需与`msedgedriver.exe`一致，路径正确。

### 7.5 采集速度慢/频繁风控？

- 换新Cookie、住宅代理、减慢采集速度，减少并发数。

---

## 8. 进一步使用

- nlp-analysis.py程序可以对output中的各个帖子进行感情色彩打分、关键词提取，最终对每个吧进行一次词频统计生成单词云图，再汇总一次得到所有文本的词频图。meta目录下存放了来自哈工大的stopwords，可以自行额外添加或替换。
- markov_generate.py程序可以将output中的文本汇总到一个大文本文档中，对其中内容进行清洗去重，并生成简单的Markov模型进行文本生成。

  > 我自己用下来这两个功能都挺鸡肋的。词频分析有很多无实义的词语混入，说明stopwords仍需完善；模型生成可能是由于数据集不够，几乎变成了文案抽取器。

---
