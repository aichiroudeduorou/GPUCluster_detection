import re
import csv
from collections import defaultdict

def parse_metrics_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # 按 #Time: 分割多个采集块
    blocks = re.split(r'^# Time:', content, flags=re.MULTILINE)[1:]

    all_rows = []
    all_metric_names = set()
    first_timestamp = None

    for block in blocks:
        lines = block.strip().split('\n')
        # 获取时间戳
        time_line = lines[0]
        time_match = re.match(r'(\d+\.\d+)', time_line)
        if not time_match:
            continue
        timestamp = float(time_match.group(1))
        if first_timestamp is None:
            first_timestamp = timestamp

        elapsed_time = timestamp - first_timestamp
        normal_duration = 0
        target = 0 if elapsed_time < normal_duration else 0

        # 按 URL 分块
        url_blocks = re.split(r'^# URL:', '\n'.join(lines[1:]), flags=re.MULTILINE)
        for url_block in url_blocks[1:]:
            url_lines = url_block.strip().split('\n')
            url_line = url_lines[0]
            # 提取IP:端口
            url_match = re.match(r'http://([\d\.]+:\d+)/metrics', url_line)
            url_ip_port = url_match.group(1) if url_match else ''
            gpu_metrics = defaultdict(dict)
            for line in url_lines[1:]:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                # 匹配 gpu="x" 或 index="x"
                metric_match = re.match(
                    r'^([^{]+)\{([^}]*)\}\s+([+-]?\d*\.?\d+)$', line)
                if not metric_match:
                    continue
                metric_name = metric_match.group(1)
                labels_str = metric_match.group(2)
                value_str = metric_match.group(3)

                # 提取 gpu_id (优先 gpu="x"，否则 index="x")
                gpu_id = None
                gpu_match = re.search(r'gpu="(\d+)"', labels_str)
                if gpu_match:
                    gpu_id = gpu_match.group(1)
                else:
                    index_match = re.search(r'index="(\d+)"', labels_str)
                    if index_match:
                        gpu_id = index_match.group(1)
                if gpu_id is None:
                    continue

                try:
                    value = float(value_str) if '.' in value_str else int(value_str)
                except ValueError:
                    value = value_str

                gpu_metrics[gpu_id][metric_name] = value
                all_metric_names.add(metric_name)

            for gpu_id, metrics in gpu_metrics.items():
                row = {'Time': timestamp, 'gpu_id': gpu_id, 'url': url_ip_port, **metrics, 'target': target}
                all_rows.append(row)

    return all_rows, sorted(all_metric_names)

def save_to_csv(rows, metric_names, output_path):
    fieldnames = ['Time', 'gpu_id', 'url'] + metric_names + ['target']
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval='')
        writer.writeheader()
        for row in rows:
            filtered_row = {k: row.get(k, '') for k in fieldnames}
            writer.writerow(filtered_row)

def swap_gpuid_url_and_replace_ip(input_csv, output_csv):
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    # 交换 gpu_id 和 url 列的位置
    if 'gpu_id' in fieldnames and 'url' in fieldnames:
        idx_gpu = fieldnames.index('gpu_id')
        idx_url = fieldnames.index('url')
        # 先移除url和gpu_id
        fieldnames.remove('gpu_id')
        fieldnames.remove('url')
        # 插入url和gpu_id，顺序交换
        fieldnames.insert(idx_gpu, 'url')
        fieldnames.insert(idx_url, 'gpu_id')

    for row in rows:
        url = row.get('url', '')
        # 替换IP并去掉端口
        if url.startswith('172.28.7.175'):
            row['url'] = '192.168.122.102'
        elif url.startswith('172.28.7.173'):
            row['url'] = '192.168.122.103'
        else:
            # 只保留IP部分（去掉端口）
            ip_match = re.match(r'([\d\.]+)', url)
            row['url'] = ip_match.group(1) if ip_match else url

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def target_adjustment_nvlink_sm(filepath, output_path, threshold_nv=0.50+1e8, threshold_sm=0.45):
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    for row in records:
        if float(row['DCGM_FI_PROF_NVLINK_RX_BYTES']) > threshold_nv or float(row['DCGM_FI_PROF_SM_ACTIVE']) > threshold_sm:
            row['target'] = 1
        else:
            row['target'] = 0
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def target_adjustment_nvlinkbandwidth(filepath, output_path, threshold=50):
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    for row in records:
        if float(row['DCGM_FI_DEV_NVLINK_BANDWIDTH_TOTAL']) > threshold:
            row['target'] = 1
        else:
            row['target'] = 0
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def target_adjustment_gpu_temp(filepath, output_path, threshold02=45, threshold03=40):    
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    start_time=None
    duration=80
    for row in records:
        if start_time is None:
            start_time = float(row['Time'])
            row['target'] = 0
            continue
        else:
            elapsed = float(row['Time']) - start_time
            if elapsed < duration:
                row['target'] = 0
                continue
        if row['IP']=='192.168.122.102' and float(row['DCGM_FI_DEV_GPU_TEMP']) < threshold02:
            row['target'] = 1
        elif row['IP']=='192.168.122.103' and float(row['DCGM_FI_DEV_GPU_TEMP']) < threshold03:
            row['target'] = 1
        else:
            row['target'] = 0
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

# ===== 使用示例 =====
input_file = '/workspace/gpu_cluster/lyc/abnormal_data/cpu/fullload/3/dcgm-1769854457.6318326'  # 替换为你的真实文件路径
output_file = '/workspace/gpu_cluster/data_processing/4090/cpu/dcgm_metrics_with_label.csv'

rows, metric_names = parse_metrics_file(input_file)
save_to_csv(rows, metric_names, output_file)
swap_gpuid_url_and_replace_ip(output_file, output_file)
# target_adjustment_gpu_temp(output_file, output_file, threshold02=45, threshold03=40)
# target_adjustment_nvlink_sm(output_file, output_file, threshold_nv=0.50+1e8, threshold_sm=0.45) # for burst
# target_adjustment_nvlinkbandwidth(output_file, output_file, threshold=50) # for oom

print(f"✅ 已解析 {len(rows)} 行 GPU 数据")
print(f"📊 涉及指标: {metric_names}")
print(f"💾 保存至: {output_file}")