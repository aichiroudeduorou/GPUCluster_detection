import re
import csv

def parse_network_file(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()

    records = []
    first_timestamp = None
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("# Time:"):
            # 提取时间戳
            time_match = re.search(r'# Time:(\d+\.\d+)', line)
            if not time_match:
                i += 1
                continue
            timestamp = float(time_match.group(1))
            if first_timestamp is None:
                first_timestamp = timestamp
            i += 1
            # 处理该时间点下的所有IP
            while i < len(lines) and not lines[i].strip().startswith("# Time"):
                ip_line = lines[i].strip()
                if ip_line.startswith("# IP:"):
                    ip_match = re.match(r'# IP:\s*([\d\.]+)', ip_line)
                    if ip_match:
                        ip = ip_match.group(1)
                        metrics = {}
                        i += 1
                        # 读取该IP下的所有指标行
                        while i < len(lines) and lines[i].strip() and not lines[i].strip().startswith("# IP") and not lines[i].strip().startswith("# Time"):
                            metric_line = lines[i].strip()
                            metric_match = re.match(r'(\w+):\s*(\d+)', metric_line)
                            if metric_match:
                                metrics[metric_match.group(1)] = int(metric_match.group(2))
                            i += 1
                        record = {'Time': timestamp, 'IP': ip}
                        record.update(metrics)
                        elapsed = timestamp - first_timestamp
                        normal_duration = 123.00
                        record['target'] = 0 if elapsed < normal_duration else 1
                        records.append(record)
                    else:
                        i += 1
                else:
                    i += 1
        else:
            i += 1
    return records

def save_to_csv(records, output_path):
    if not records:
        raise ValueError("未解析到任何有效数据！")

    # 获取所有字段名（保持 Time 在前，target 在后）
    fieldnames = ['Time']
    # 收集所有指标名（排除 Time 和 target）
    metric_keys = set()
    for rec in records:
        for k in rec.keys():
            if k not in ('Time', 'target'):
                metric_keys.add(k)
    fieldnames += sorted(metric_keys)  # 或按出现顺序
    fieldnames.append('target')

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval='')
        writer.writeheader()
        for row in records:
            # 补全缺失字段（理论上不会缺）
            full_row = {k: row.get(k, '') for k in fieldnames}
            writer.writerow(full_row)

def parse_normal_intervals(config_str):
    intervals = []
    current_time = 0
    # 分割字符串 "2:600,200:10..."
    parts = config_str.strip().split(',')
    
    for part in parts:
        if not part: continue
        # 解析 "请求数:持续时间"
        req_count, duration = map(int, part.split(':'))
        
        end_time = current_time + duration
        
        # 如果请求数为 1 或 2，则视为正常阶段
        if req_count in [1, 2]:
            intervals.append([current_time, end_time])
            
        current_time = end_time
    
    return intervals

config_data = "2:600,200:10,1:300,100:15,1:450,150:10,2:600"
normal_duration = parse_normal_intervals(config_data)

# 调整 CSV 文件中的 target 列, normal_duration为正常区间列表，将正常区间内的 target 设为 0，异常区间的target设为1
def target_adjustment_duration(filepath, output_path, normal_duration):
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    first_timestamp = None
    for row in records:
        timestamp = float(row['Time'])
        if first_timestamp is None:
            first_timestamp = timestamp
        # 判断 timestamp 是否在任何正常区间内
        is_normal = any(start <= (timestamp-first_timestamp) < end for start, end in normal_duration)
        row['target'] = 0 if is_normal else 1
    # 保存调整后的文件
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def target_adjustment_rxpackets(filepath, output_path, threshold_h,threshold_l):
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    pri_02 = 0
    pri_03 = 0
    for row in records:
        '''
        if row['IP']=='192.168.122.102':
            if pri_02 ==0:
                pri_02 = int(row['rx_packets'])
                row['target'] = 0
            elif abs(int(row['rx_packets']) - pri_02)/int(row['rx_packets']) > 0.9 or int(row['rx_packets']) <50:
                row['target'] = 1
                pri_02 = int(row['rx_packets'])
            else:
                row['target'] = 0
        else:
            if row['IP']=='192.168.122.103':
                if pri_03 ==0:
                    pri_03 = int(row['rx_packets'])
                    row['target'] = 0
                elif abs(int(row['rx_packets']) - pri_03)/int(row['rx_packets']) > 0.9 or int(row['rx_packets']) <50:
                    row['target'] = 1
                    pri_03 = int(row['rx_packets'])
                else:
                    row['target'] = 0
        '''
        if row['IP']=='192.168.122.102':
            if int(row['rx_packets']) > threshold_l or int(row['rx_packets']) <50:
                row['target'] = 1
            else:
                row['target'] = 0
        else:
            if row['IP']=='192.168.122.103':
                if int(row['rx_packets']) > threshold_h or int(row['rx_packets']) <50:
                    row['target'] = 1
                else:
                    row['target'] = 0
    # 保存调整后的文件
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def target_adjustment_txbytes(filepath, output_path, threshold=50000):
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    for row in records:
        if int(row['tx_bytes']) > threshold:
            row['target'] = 1
        else:
            row['target'] = 0
    # 保存调整后的文件
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

# ===== 使用示例 =====
input_file = "/workspace/lyc/abnormal_data/network-error/collapse_caused_by_speed/network-1765122029.9178896"      # 替换为你的实际文件名
output_file = "/workspace/gpu_cluster/data_processing/4090/network/network_metrics_labeled.csv"

records = parse_network_file(input_file)
save_to_csv(records, output_file)
# target_adjustment_rxpackets(output_file, output_file, threshold=300) # for burst
# target_adjustment_txbytes(output_file, output_file, threshold=50000) # for oom
target_adjustment_rxpackets(output_file, output_file, threshold_h=300000, threshold_l=8000) # for 4090 oom
print(f"✅ 成功解析 {len(records)} 个时间点")
print(f"💾 已保存带标签的 CSV 文件：{output_file}")

# 可选：打印前几行验证
for r in records[:2]:
    print(r)