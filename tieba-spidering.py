import os
import re
import time
import random
import json
import threading
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from bs4 import BeautifulSoup
import redis

MY_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
LOG_FILE = "tieba_crawler.log"
EDGE_DRIVER_PATH = r"C:\Users\30522\Desktop\Coding\vscode\msedgedriver.exe"
RECOVER_FILE = "resume_info.json"
COOKIES_TXT = "cookies.txt"
PROXIES_TXT = "proxies.txt"
TASKS_REDIS_KEY = "tieba_tasks"
TASKS_INPROGRESS = "tasks_inprogress.txt"
TASKS_DONE = "tasks_done.txt"

SLEEP_PAGE = (5, 10)
SLEEP_THREAD = (3, 8)
SLEEP_RETRY = 30
CAPTCHA_TIMEOUT = 300      # 5分钟卡人工验证自动切换cookie
ALL_COOKIE_TIMEOUT = 1200  # 20分钟所有cookie失效后自动停下
PROXY_COOLDOWN_TIME = 600  # 10分钟，被风控后该IP冷却时间（秒）

# ========== Redis配置 ==========
REDIS_HOST = "localhost"  # 修改为你的Redis主机
REDIS_PORT = 6379
REDIS_DB = 0
# ==============================

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def safe_filename(name):
    return re.sub(r'[\\/:*?"<>|]', '_', name)

def save_resume_info(resume_info):
    with open(RECOVER_FILE, "w", encoding="utf-8") as f:
        json.dump(resume_info, f, ensure_ascii=False, indent=2)

def load_resume_info():
    if os.path.exists(RECOVER_FILE):
        with open(RECOVER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def load_cookie_list():
    if os.path.exists(COOKIES_TXT):
        with open(COOKIES_TXT, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return []

def load_proxy_list():
    if os.path.exists(PROXIES_TXT):
        with open(PROXIES_TXT, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return []

def setup_driver(headless=True, proxy=None):
    edge_options = Options()
    if headless:
        edge_options.add_argument('--headless')
    edge_options.add_argument(f'user-agent={MY_UA}')
    edge_options.add_argument("--disable-blink-features=AutomationControlled")
    if proxy and not proxy.endswith(':0'):
        edge_options.add_argument(f'--proxy-server=http://{proxy}')
    service = Service(EDGE_DRIVER_PATH)
    driver = webdriver.Edge(service=service, options=edge_options)
    driver.set_page_load_timeout(60)
    return driver

def add_cookies(driver, cookie_str):
    driver.get("https://tieba.baidu.com/")
    time.sleep(3)
    for pair in cookie_str.split(";"):
        if '=' in pair:
            k, v = pair.strip().split("=", 1)
            cookie_dict = {'name': k, 'value': v}
            try:
                driver.add_cookie(cookie_dict)
            except Exception as e:
                log(f"添加Cookie失败: {cookie_dict} 错误: {e}")

def is_need_captcha(driver):
    text = driver.page_source
    keywords = [
        "安全验证", "人机验证", "请输入验证码", "系统检测到您的请求存在异常"
    ]
    for word in keywords:
        if word in text:
            return True
    return False

def is_cookie_expired(driver):
    text = driver.page_source
    keywords = [
        "登录百度帐号", "请在手机上确认登录"
    ]
    for word in keywords:
        if word in text:
            return True
    return False

def wait_for_manual_captcha_with_timeout(driver, url, stage_desc="", timeout=CAPTCHA_TIMEOUT):
    log(f"检测到安全验证（{stage_desc}），已弹出浏览器，请在页面中手动完成验证，然后按回车继续（限时{timeout//60}分钟）")
    driver.quit()
    driver = setup_driver(headless=False)
    driver.get(url)
    user_input = {"done": False}

    def wait_input():
        input("完成安全验证后请按回车继续...（超时会自动切换cookie）")
        user_input["done"] = True

    t = threading.Thread(target=wait_input)
    t.daemon = True
    t.start()
    t.join(timeout)
    if t.is_alive():
        log(f"超时{timeout//60}分钟无人操作，自动切换到下一个Cookie！")
        return driver, False
    else:
        log("人工验证已完成，继续任务。")
        return driver, True

def has_next_page(soup):
    for a in soup.find_all('a'):
        if a.text.strip() in ['下一页', '下一页 >', '下一页›']:
            return True
    return False

def is_valid_speech(text):
    if len(text) < 8:
        return False
    if not any('\u4e00' <= ch <= '\u9fff' for ch in text):
        return False
    keywords = [
        '吧务提醒', '签到', '本帖最后由', '回复：', '引用', '客户端', '推广', '广告', '[图片]', '[表情]', 'img', '楼主'
    ]
    for k in keywords:
        if k in text:
            return False
    if re.search(r'\[.+?\]', text):
        return False
    return True

def get_lzl_comments_if_exist(driver, tid, pid, post):
    lzl_entry = post.find('a', attrs={'class': 'j_lzl_s_p'})
    if not lzl_entry:
        return []
    lzl_comments = []
    lzl_pn = 1
    while True:
        comment_url = f"https://tieba.baidu.com/p/comment?tid={tid}&pid={pid}&pn={lzl_pn}"
        driver.get(comment_url)
        time.sleep(1.5)
        comment_soup = BeautifulSoup(driver.page_source, 'html.parser')
        comment_texts = comment_soup.find_all('span', attrs={'class': 'lzl_content_main'})
        if not comment_texts:
            break
        for c in comment_texts:
            cmt = c.get_text(strip=True)
            if is_valid_speech(cmt):
                lzl_comments.append(cmt)
        next_lzl = False
        for a in comment_soup.find_all('a'):
            if a.text.strip() in ['下一页', '下一页 >', '下一页›']:
                next_lzl = True
                break
        if next_lzl:
            lzl_pn += 1
        else:
            break
    return lzl_comments

def get_thread_content_selenium(thread_url, driver, max_floors=100, bar_name="", thread_title=""):
    content = []
    pn = 1
    total_floors = 0
    tid = thread_url.split('/p/')[1].split('?')[0]
    while True:
        url = thread_url + f'?pn={pn}'
        driver.get(url)
        time.sleep(random.uniform(*SLEEP_THREAD))
        if is_cookie_expired(driver):
            raise Exception("CookieExpired")
        if is_need_captcha(driver):
            raise Exception(f"NeedCaptcha::{url}")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        posts = soup.find_all('div', attrs={'class': 'l_post l_post_bright j_l_post clearfix'})
        if not posts:
            log(f"[{bar_name}] 帖子[{thread_title}]第{pn}页未发现发言内容，打印源码片段：")
            log(driver.page_source[:1500])
            break
        for post in posts:
            main_content_div = post.find('div', attrs={'class': 'd_post_content'})
            if not main_content_div:
                continue
            text = main_content_div.get_text(strip=True)
            try:
                data_field = json.loads(post.get('data-field', '{}'))
                floor_num = data_field.get("content", {}).get("post_no", "")
                pid = data_field.get("content", {}).get("post_id", "")
            except Exception:
                floor_num = ""
                pid = ""
            if is_valid_speech(text):
                content.append(text)
            if pid:
                comments = get_lzl_comments_if_exist(driver, tid, pid, post)
                content.extend(comments)
            total_floors += 1
            if total_floors >= max_floors:
                log(f"[{bar_name}] 帖子[{thread_title}]已到达楼层上限 {max_floors}")
                return '\n'.join(content)
        if has_next_page(soup) and total_floors < max_floors:
            pn += 1
            time.sleep(random.uniform(*SLEEP_THREAD))
        else:
            break
    return '\n'.join(content)

def get_thread_list_selenium(bar_name, page=1, driver=None):
    url = f'https://tieba.baidu.com/f?kw={bar_name}&pn={(page-1)*50}'
    driver.get(url)
    time.sleep(random.uniform(*SLEEP_PAGE))
    if is_cookie_expired(driver):
        raise Exception("CookieExpired")
    if is_need_captcha(driver):
        raise Exception(f"NeedCaptcha::{url}")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    threads = []
    for item in soup.select('a[href^="/p/"]'):
        title = item.get('title') or item.text.strip()
        href = item.get('href')
        if title and href and href.startswith('/p/'):
            threads.append({'title': title, 'url': 'https://tieba.baidu.com' + href})
    log(f"[{bar_name}] 第{page}页解析到 {len(threads)} 个帖子")
    if not threads:
        log(f"[{bar_name}] 第{page}页未解析到帖子，打印页面部分源码供调试：")
        log(driver.page_source[:1500])
    return threads

# ============== Redis分布式任务池 ==============

def get_redis_conn():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def get_one_task():
    """
    从Redis队列里原子领取一个任务（所有进程自然不会重复）
    返回dict或None
    """
    r = get_redis_conn()
    task_line = r.lpop(TASKS_REDIS_KEY)
    if not task_line:
        return None
    with open(TASKS_INPROGRESS, "a", encoding="utf-8") as f:
        f.write(task_line + "\n")
    return json.loads(task_line)

def mark_task_done(task):
    task_line = json.dumps(task, ensure_ascii=False)
    with open(TASKS_DONE, "a", encoding="utf-8") as f:
        f.write(task_line + "\n")

def batch_crawl_tieba_selenium(bar_list, max_pages=1, start_page=1, save_dir='output', max_floors_per_thread=100):
    log(f"==== 本次批量爬取任务开始 ====")
    resume_info = load_resume_info() or {"bar_idx": 0, "page": start_page, "thread_idx": 0}
    bar_idx = resume_info.get("bar_idx", 0)
    page = resume_info.get("page", start_page)
    thread_idx = resume_info.get("thread_idx", 0)
    cookie_list = load_cookie_list()
    proxy_list = load_proxy_list()
    proxy_cooldown_dict = {}
    cookie_idx = 0
    proxy_idx = 0
    all_cookie_fail_time = None

    while True:
        cookie_list = load_cookie_list()
        proxy_list = load_proxy_list()
        proxy_tried = 0
        while proxy_tried < len(proxy_list):
            now = time.time()
            curr_proxy = proxy_list[proxy_idx % len(proxy_list)]
            cooldown_end = proxy_cooldown_dict.get(curr_proxy, 0)
            if now < cooldown_end:
                proxy_idx += 1
                proxy_tried += 1
            else:
                break
        else:
            if proxy_cooldown_dict:
                soonest_ready = min(proxy_cooldown_dict.values())
                wait_time = int(soonest_ready - time.time()) + 1
                log(f"所有代理IP都在冷却中，等待{wait_time}秒后重试...")
                time.sleep(wait_time)
                continue

        if cookie_idx >= len(cookie_list):
            if all_cookie_fail_time is None:
                all_cookie_fail_time = time.time()
            elapsed = time.time() - all_cookie_fail_time
            if elapsed >= ALL_COOKIE_TIMEOUT:
                log("所有Cookie失效超时20分钟，程序自动退出。")
                return
            log("所有Cookie已失效，请在 cookies.txt 中补充新的 Cookie！")
            log(f"等待人工补充Cookie...剩余{int((ALL_COOKIE_TIMEOUT - elapsed)//60)}分")
            for _ in range(60):
                time.sleep(1)
                cookie_list = load_cookie_list()
                if len(cookie_list) > 0:
                    log(f"检测到新Cookie条目{len(cookie_list)}条，尝试继续爬虫...")
                    cookie_idx = 0
                    all_cookie_fail_time = None
                    break
            continue

        current_cookie = cookie_list[cookie_idx]
        curr_proxy = proxy_list[proxy_idx % len(proxy_list)] if proxy_list else None
        log(f"当前使用代理IP: {curr_proxy if curr_proxy else '无'}，Cookie索引: {cookie_idx}")
        driver = setup_driver(proxy=curr_proxy)
        add_cookies(driver, current_cookie)
        time.sleep(2)
        try:
            for i in range(bar_idx, len(bar_list)):
                bar = bar_list[i]
                threads_per_page = None
                for p in range(page, max_pages + 1):
                    try:
                        threads = get_thread_list_selenium(bar, p, driver)
                        threads_per_page = threads
                    except Exception as e:
                        if str(e).startswith("NeedCaptcha::"):
                            url = str(e).split("::", 1)[1]
                            driver, solved = wait_for_manual_captcha_with_timeout(driver, url, f"吧[{bar}]第{p}页")
                            if solved:
                                threads = get_thread_list_selenium(bar, p, driver)
                                threads_per_page = threads
                            else:
                                driver.quit()
                                if curr_proxy:
                                    proxy_cooldown_dict[curr_proxy] = time.time() + PROXY_COOLDOWN_TIME
                                    log(f"代理{curr_proxy}遇到风控，加入冷却{PROXY_COOLDOWN_TIME//60}分钟。切换下一个代理和cookie。")
                                    proxy_idx += 1
                                else:
                                    log("遇到风控，但未使用代理，仅切换cookie。")
                                cookie_idx += 1
                                save_resume_info({"bar_idx": i, "page": p, "thread_idx": thread_idx})
                                raise Exception("ProxyOrCookieCooldown")
                        elif "CookieExpired" in str(e):
                            raise
                        else:
                            raise
                    t_start = thread_idx if (i == bar_idx and p == page) else 0
                    for t_idx in range(t_start, len(threads_per_page)):
                        thread = threads_per_page[t_idx]
                        filename = safe_filename(thread['title']) + '.txt'
                        bar_dir = os.path.join(save_dir, safe_filename(bar))
                        filepath = os.path.join(bar_dir, filename)
                        if os.path.exists(filepath):
                            log(f"[{bar}] 帖子[{thread['title']}]已存在，跳过")
                            save_resume_info({"bar_idx": i, "page": p, "thread_idx": t_idx + 1})
                            continue
                        try:
                            content = get_thread_content_selenium(
                                thread['url'], driver, max_floors=max_floors_per_thread,
                                bar_name=bar, thread_title=thread['title']
                            )
                            os.makedirs(bar_dir, exist_ok=True)
                            with open(filepath, 'w', encoding='utf-8') as f:
                                f.write(content)
                            log(f"[{bar}] 帖子[{thread['title']}]已保存：{filepath}")
                        except Exception as e:
                            if str(e).startswith("NeedCaptcha::"):
                                url = str(e).split("::", 1)[1]
                                driver, solved = wait_for_manual_captcha_with_timeout(driver, url, f"吧[{bar}]帖子[{thread['title']}]")
                                if solved:
                                    content = get_thread_content_selenium(
                                        thread['url'], driver, max_floors=max_floors_per_thread,
                                        bar_name=bar, thread_title=thread['title']
                                    )
                                    os.makedirs(bar_dir, exist_ok=True)
                                    with open(filepath, 'w', encoding='utf-8') as f:
                                        f.write(content)
                                    log(f"[{bar}] 帖子[{thread['title']}]已保存：{filepath}")
                                else:
                                    driver.quit()
                                    if curr_proxy:
                                        proxy_cooldown_dict[curr_proxy] = time.time() + PROXY_COOLDOWN_TIME
                                        log(f"代理{curr_proxy}遇到风控，加入冷却{PROXY_COOLDOWN_TIME//60}分钟。切换下一个代理和cookie。")
                                        proxy_idx += 1
                                    else:
                                        log("遇到风控，但未使用代理，仅切换cookie。")
                                    cookie_idx += 1
                                    save_resume_info({"bar_idx": i, "page": p, "thread_idx": t_idx})
                                    raise Exception("ProxyOrCookieCooldown")
                            elif "CookieExpired" in str(e):
                                save_resume_info({"bar_idx": i, "page": p, "thread_idx": t_idx})
                                raise
                            else:
                                log(f"[{bar}] 帖子[{thread['title']}]保存失败: {e}")
                        save_resume_info({"bar_idx": i, "page": p, "thread_idx": t_idx + 1})
                        time.sleep(random.uniform(*SLEEP_THREAD))
                    thread_idx = 0
                page = 1
            driver.quit()
            if os.path.exists(RECOVER_FILE):
                os.remove(RECOVER_FILE)
            break
        except Exception as e:
            driver.quit()
            if "ProxyOrCookieCooldown" in str(e):
                continue
            if "CookieExpired" in str(e):
                log(f"Cookie已失效，自动切换到下一个Cookie：{cookie_idx+1}")
                cookie_idx += 1
                continue
            else:
                log(f"遇到其他异常：{e}")
                break
    log(f"==== 本次批量爬取任务结束 ====")

if __name__ == '__main__':
    max_floors_per_thread = 200
    while True:
        task = get_one_task()
        if not task:
            log("没有可领取的任务，爬虫退出。")
            break
        try:
            bar_name = task['bar']
            start_page = task.get('page_start', 1)
            end_page = task.get('page_end', start_page)
            batch_crawl_tieba_selenium(
                [bar_name],
                max_pages=end_page,
                start_page=start_page,
                max_floors_per_thread=max_floors_per_thread
            )
            mark_task_done(task)
        except Exception as e:
            log(f"采集任务失败: {task}, 错误: {e}")