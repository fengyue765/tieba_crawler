import os
import jieba
import jieba.analyse
from collections import Counter, defaultdict
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from snownlp import SnowNLP
import re

def load_stopwords(*files):
    stopwords = set()
    for file in files:
        if not os.path.isfile(file):
            continue
        with open(file, encoding="utf-8") as f:
            for line in f:
                w = line.strip()
                if w:
                    stopwords.add(w)
    stopwords |= set("，。！？；：、“”‘’（）—…《》【】[](){}|\\/~`·+=-_——,.!?;:\"'<> \t\r\n")
    return stopwords

STOPWORDS = load_stopwords("scu_stopwords.txt", "cn_stopwords.txt")

def clean_text(text):
    text = text.replace('\r', '').replace('\n', ' ')
    for tag in ['[图片]', '[表情]', '[广告]']:
        text = text.replace(tag, '')
    text = re.sub(r'(.)\1{4,}', r'\1', text)
    text = re.sub(r'([^\w\s])\1{2,}', r'\1', text)
    meaningless_patterns = [
        r'(支持|顶|沙发|路过|关注|前排|占座|加油|up|mark|围观|感谢|谢谢|收藏|学习|帮顶|dddd|dddddd|6+|666+|233+){2,}',
        r'(作者已被贴吧屏蔽|点击展开|查看完整图片|本吧发帖|贴吧用户_[0-9a-zA-Z]+|来自:.*客户端|发自.*客户端|已阅|已读|已收藏|已关注)',
    ]
    for pat in meaningless_patterns:
        text = re.sub(pat, '', text, flags=re.IGNORECASE)
    text = re.sub(r'(.{4,20})\1{2,}', r'\1', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

def filter_words(words, stopwords=STOPWORDS):
    return [
        w for w in words
        if w not in stopwords and not re.match(r'^[\s\W_]+$', w) and w.strip() != ""
    ]

def analyze_file(filepath):
    with open(filepath, encoding="utf-8") as f:
        raw = f.read()
    text = clean_text(raw)
    words = jieba.lcut(text)
    words = filter_words(words)
    keywords = jieba.analyse.extract_tags(' '.join(words), topK=10)
    s = SnowNLP(text)
    sentiment = s.sentiments
    return {
        "words": words,
        "keywords": keywords,
        "sentiment": sentiment,
        "text": text
    }

def generate_wordcloud(freq, font_path="msyh.ttc", out_path=None):
    wc = WordCloud(font_path=font_path, width=800, height=400, background_color='white').generate_from_frequencies(freq)
    plt.imshow(wc)
    plt.axis("off")
    if out_path:
        wc.to_file(out_path)
    else:
        plt.show()

def ensure_dir(d):
    if not os.path.exists(d):
        os.makedirs(d)

def main(output_dir="output", result_dir="nlp-result", wordcloud_font="msyh.ttc"):
    ensure_dir(result_dir)
    all_words = []
    file_results = []
    bar_words = defaultdict(list)
    bar_file_results = defaultdict(list)
    print("批量分析 output 目录下所有帖子...")

    for bar in os.listdir(output_dir):
        bar_path = os.path.join(output_dir, bar)
        if not os.path.isdir(bar_path): continue
        for fname in os.listdir(bar_path):
            fpath = os.path.join(bar_path, fname)
            try:
                result = analyze_file(fpath)
                all_words += result["words"]
                file_results.append({
                    "bar": bar,
                    "file": fname,
                    "keywords": result["keywords"],
                    "sentiment": result["sentiment"]
                })
                # 各吧数据
                bar_words[bar] += result["words"]
                bar_file_results[bar].append({
                    "file": fname,
                    "keywords": result["keywords"],
                    "sentiment": result["sentiment"]
                })
                print(f"[{bar}/{fname}] 关键词: {result['keywords']} 情感分: {result['sentiment']:.2f}")
            except Exception as e:
                print(f"处理文件 {fpath} 出错: {e}")

    # 全局词频统计
    freq = Counter(all_words)
    print("\n全局高频词TOP20：")
    print(freq.most_common(20))
    try:
        generate_wordcloud(freq, font_path=wordcloud_font, out_path=os.path.join(result_dir, "wordcloud.png"))
        print(f"全局词云已保存为 {os.path.join(result_dir, 'wordcloud.png')}")
    except Exception as e:
        print("全局词云生成失败（可能字体文件不存在）：", e)

    with open(os.path.join(result_dir, "nlp_analysis_results.txt"), "w", encoding="utf-8") as f:
        for item in file_results:
            f.write(f"{item['bar']}/{item['file']}\t关键词:{','.join(item['keywords'])}\t情感分:{item['sentiment']:.2f}\n")
    print(f"全局分析结果已保存到 {os.path.join(result_dir, 'nlp_analysis_results.txt')}")

    # === 各吧统计 ===
    for bar in bar_words:
        bar_freq = Counter(bar_words[bar])
        print(f"\n吧：{bar} 高频词TOP20：")
        print(bar_freq.most_common(20))
        # 各吧词云
        try:
            generate_wordcloud(bar_freq, font_path=wordcloud_font, out_path=os.path.join(result_dir, f"wordcloud_{bar}.png"))
            print(f"{bar} 词云已保存为 {os.path.join(result_dir, f'wordcloud_{bar}.png')}")
        except Exception as e:
            print(f"{bar} 词云生成失败：", e)
        # 各吧分析汇总
        with open(os.path.join(result_dir, f"nlp_analysis_results_{bar}.txt"), "w", encoding="utf-8") as f:
            for item in bar_file_results[bar]:
                f.write(f"{bar}/{item['file']}\t关键词:{','.join(item['keywords'])}\t情感分:{item['sentiment']:.2f}\n")
        print(f"{bar} 分析汇总已保存到 {os.path.join(result_dir, f'nlp_analysis_results_{bar}.txt')}")

if __name__ == "__main__":
    main()