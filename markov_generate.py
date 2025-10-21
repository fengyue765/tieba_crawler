import markovify
import os
import pickle
import re
import jieba
import random
import json
import OpenHowNet
from collections import Counter, defaultdict
from tqdm import tqdm

# === 目录配置 ===
DATA_DIR = "data"
MODEL_DIR = "models"
META_DIR = "meta"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

# === 加载OpenHowNet和近反义词典 ===
hownet_dict = OpenHowNet.HowNetDict()
try:
    with open(os.path.join(META_DIR, "synonyms.json"), encoding="utf-8") as f:
        SYNONYMS = json.load(f)
except:
    SYNONYMS = {}
try:
    with open(os.path.join(META_DIR, "antonyms.json"), encoding="utf-8") as f:
        ANTONYMS = json.load(f)
except:
    ANTONYMS = {}

def clean_text(text):
    text = text.replace('\r', '').replace('\n', ' ')
    for tag in ['[图片]', '[表情]', '[广告]']:
        text = text.replace(tag, '')
    text = re.sub(r'(.)\1{4,}', r'\1', text)
    text = re.sub(r'([^\w\s])\1{2,}', r'\1', text)
    meaningless_patterns = [
        r'(支持|顶|沙发|路过|关注|前排|占座|加油|up|mark|围观|感谢|谢谢|收藏|学习|帮顶|dddd|dddddd|6+|666+|233+){2,}',
        r'(作者已被贴吧屏蔽|点击展开|查看完整图片| ۣۣۖۖิ| ۖิ|本吧发帖|贴吧用户_[0-9a-zA-Z]+|来自:.*客户端|发自.*客户端|已阅|已读|已收藏|已关注)',
    ]
    for pat in meaningless_patterns:
        text = re.sub(pat, '', text, flags=re.IGNORECASE)
    text = re.sub(r'(.{4,20})\1{2,}', r'\1', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

def get_synonyms(word):
    syns = SYNONYMS.get(word, None)
    if syns is not None and len(syns) > 0:
        return syns
    try:
        syns = [w for w, _ in hownet_dict.get_nearest_words(word, topn=10) if w != word]
    except Exception:
        syns = []
    return syns

def get_antonyms(word):
    ants = ANTONYMS.get(word, None)
    if ants is not None and len(ants) > 0:
        return ants
    if hasattr(hownet_dict, "get_antonym"):
        try:
            ants = hownet_dict.get_antonym(word)
        except Exception:
            ants = []
        if ants:
            return ants
    return []

def synonym_replace(sentence, prob=0.15):
    words = list(jieba.cut(sentence))
    new_words = []
    for w in words:
        if random.random() < prob:
            syns = get_synonyms(w)
            if syns:
                new_words.append(random.choice(syns))
                continue
        new_words.append(w)
    return "".join(new_words)

def antonym_negate_replace(sentence, prob=0.10):
    words = list(jieba.cut(sentence))
    new_words = []
    for w in words:
        if random.random() < prob:
            ants = get_antonyms(w)
            if ants:
                new_words.append("不" + random.choice(ants))
                continue
        new_words.append(w)
    return "".join(new_words)

def merge_txt_files_to_corpus(output_dir="output"):
    all_lines = set()
    corpus_file = os.path.join(DATA_DIR, "all_posts.txt")
    with open(corpus_file, "w", encoding="utf-8") as fout:
        files = [f for f in os.listdir(output_dir) if f.endswith(".txt")]
        for fname in files:
            fpath = os.path.join(output_dir, fname)
            with open(fpath, encoding="utf-8") as fin:
                for line in fin:
                    line = clean_text(line.strip())
                    if line and line not in all_lines:
                        fout.write(line + "\n")
                        all_lines.add(line)
    print(f"已合并清洗所有文件到 {corpus_file}")

def deduplicate_by_length_bucket_parallel(
    input_file_or_files, 
    output_file, 
    sim_threshold=0.95, 
    bucket_size=10, 
    max_bucket_size=1000, 
    sub_bucket_count=10, 
    num_workers=8
):
    lines = []
    files = input_file_or_files if isinstance(input_file_or_files, (list, tuple)) else [input_file_or_files]
    seen = set()
    for file in files:
        if not os.path.exists(file):
            continue
        with open(file, encoding="utf-8") as fin:
            for line in fin:
                line = line.strip()
                if line and line not in seen:
                    lines.append(line)
                    seen.add(line)
    print(f"读取总语料数：{len(lines)}")
    buckets = defaultdict(list)
    for idx, line in enumerate(lines):
        l = len(line)
        buckets[l // bucket_size].append((idx, line))
    print(f"初步分为 {len(buckets)} 个长度桶，每桶宽度 {bucket_size}")
    def further_bucket_by_hash(group, sub_bucket_count=10):
        sub_buckets = defaultdict(list)
        for idx, line in group:
            h = hash(line) % sub_bucket_count
            sub_buckets[h].append((idx, line))
        return list(sub_buckets.values())
    all_groups = []
    for group in buckets.values():
        if len(group) > max_bucket_size:
            sub_groups = further_bucket_by_hash(group, sub_bucket_count=sub_bucket_count)
            all_groups.extend([g for g in sub_groups if g])
        else:
            all_groups.append(group)
    print(f"最终并发分组数（桶+子桶）：{len(all_groups)}")
    def jaccard_sim(s1, s2):
        set1, set2 = set(s1), set(s2)
        if not set1 or not set2:
            return 0.0
        return len(set1 & set2) / len(set1 | set2)
    def bucket_worker(group, sim_threshold):
        removed = set()
        n = len(group)
        for i in range(n):
            idx_i, line_i = group[i]
            if idx_i in removed:
                continue
            for j in range(i+1, n):
                idx_j, line_j = group[j]
                if idx_j in removed:
                    continue
                sim = jaccard_sim(line_i, line_j)
                if sim >= sim_threshold:
                    if len(line_i) <= len(line_j):
                        removed.add(idx_i)
                    else:
                        removed.add(idx_j)
        return removed
    all_removed = set()
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(bucket_worker, group, sim_threshold): group for group in all_groups}
        for i, future in enumerate(tqdm(as_completed(futures), total=len(futures), desc="桶去重进度")):
            removed = future.result()
            all_removed.update(removed)
    with open(output_file, "w", encoding="utf-8") as fout:
        for idx, line in enumerate(lines):
            if idx not in all_removed:
                fout.write(line + "\n")
    print(f"分桶多线程相似度去重完成，剩余句子数：{len(lines)-len(all_removed)}，结果保存至{output_file}")

def train_markov_model(corpus_file, model_file):
    with open(corpus_file, encoding="utf-8") as f:
        text = f.read()
    model = markovify.NewlineText(text, retain_original=False, state_size=1)
    with open(model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"模型已保存到 {model_file}")
    return model

def load_model(model_file):
    with open(model_file, "rb") as f:
        model = pickle.load(f)
    print(f"模型已从 {model_file} 加载")
    return model

def select_data_file():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.txt')]
    if not files:
        print("未找到任何数据文件，请先生成语料。")
        return None
    print("可用数据文件：")
    for idx, f in enumerate(files):
        print(f"{idx+1}. {f}")
    num = input(f"选择数据文件编号 (1-{len(files)}, 默认1)：").strip()
    if num.isdigit() and 1 <= int(num) <= len(files):
        return os.path.join(DATA_DIR, files[int(num)-1])
    return os.path.join(DATA_DIR, files[0])

def select_model_file():
    models = [f for f in os.listdir(MODEL_DIR) if f.endswith('.pkl')]
    if not models:
        print("未找到任何模型文件，请先训练模型。")
        return None
    print("可用模型文件：")
    for idx, m in enumerate(models):
        print(f"{idx+1}. {m}")
    num = input(f"选择模型编号 (1-{len(models)}, 默认1)：").strip()
    if num.isdigit() and 1 <= int(num) <= len(models):
        return os.path.join(MODEL_DIR, models[int(num)-1])
    return os.path.join(MODEL_DIR, models[0])

def generate_sentences(
    model, n=5, keyword=None, tries=100, keyword_tries=30,
    do_synonym=True, do_antonym=True, synonym_prob=0.15, antonym_prob=0.10, max_len=100
):
    count = 0
    generated = 0
    results = []
    while generated < n and count < n * keyword_tries * 2:
        sentence = model.make_sentence()
        if sentence is None or (keyword and keyword not in sentence):
            count += 1
            continue
        new_sentence = sentence
        if do_synonym:
            new_sentence = synonym_replace(new_sentence, prob=synonym_prob)
        if do_antonym:
            new_sentence = antonym_negate_replace(new_sentence, prob=antonym_prob)
        clean_sentence = clean_text(new_sentence)
        if len(clean_sentence) > max_len:
            count += 1
            continue
        print(f"生成文案{generated+1}：{clean_sentence}")
        results.append(clean_sentence)
        generated += 1
        count += 1
    if generated < n:
        print(f"（仅生成{generated}条符合要求的文案）")
        if generated == 0:
            print("== 没有生成任何句子，可能原因：语料太少、条件过苛刻 ==")
    return results

def main():
    print("请选择功能：")
    print("1. 合并并清洗output目录下文本为大语料")
    print("2. 合并去重所有语料为唯一大语料")
    print("3. 训练模型（可自定义训练集和模型名）")
    print("4. 用指定模型生成文案")
    choice = input("输入功能编号 (1~4)：").strip()

    if choice == "1":
        merge_txt_files_to_corpus(output_dir="output")
    elif choice == "2":
        files = [
            os.path.join(DATA_DIR, "all_posts.txt"),
            os.path.join(DATA_DIR, "all_posts_augmented.txt"),
            os.path.join(DATA_DIR, "all_posts_bayes.txt"),
        ]
        deduplicate_by_length_bucket_parallel(
            input_file_or_files=files,
            output_file=os.path.join(DATA_DIR, "all_posts_merged_dedup.txt"),
            sim_threshold=0.88,
            bucket_size=10,
            max_bucket_size=1000,
            sub_bucket_count=10,
            num_workers=8
        )
        print("建议后续训练直接用 all_posts_merged_dedup.txt")
    elif choice == "3":
        data_file = select_data_file()
        if not data_file:
            return
        model_name = input("请输入要保存的模型文件名（如 my_model.pkl）：").strip()
        if not model_name.endswith(".pkl"):
            model_name += ".pkl"
        model_file = os.path.join(MODEL_DIR, model_name)
        train_markov_model(corpus_file=data_file, model_file=model_file)
        print(f"已用 {data_file} 训练并保存模型为 {model_file}")
    elif choice == "4":
        model_file = select_model_file()
        if not model_file:
            return
        model = load_model(model_file)
        print("请选择生成方式：")
        print("1. 随机生成")
        print("2. 指定关键词生成（含关键词）")
        sub_choice = input("输入数字选择 (1/2)：").strip()
        num = input("生成多少句？(默认5)：").strip()
        try:
            num = int(num)
        except:
            num = 5
        if sub_choice == "1":
            generate_sentences(model, n=num)
        elif sub_choice == "2":
            keyword = input("请输入关键词（句中必须包含）：").strip()
            generate_sentences(model, n=num, keyword=keyword)
        else:
            print("无效选项。")
    else:
        print("无效选项，请重试。")

if __name__ == "__main__":
    main()