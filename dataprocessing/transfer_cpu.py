import re
import csv

def parse_cpu_metrics_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    blocks = re.split(r'^# Time:', content, flags=re.MULTILINE)[1:]
    all_rows = []
    metric_order = []
    first_timestamp = None

    for block in blocks:
        lines = block.strip().split('\n')
        if not lines:
            continue
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
        target = 0 if elapsed_time < normal_duration else 1

        # 查找所有CPU段
        cpu_blocks = re.split(r'^# (Local|Remote) CPU:\s*([\d\.]+)', '\n'.join(lines[1:]), flags=re.MULTILINE)
        # cpu_blocks[0]为''，后面每两项为类型(local/remote)和IP，内容为下一项
        for i in range(1, len(cpu_blocks), 3):
            cpu_type = cpu_blocks[i].strip().lower()  # local 或 remote
            ip = cpu_blocks[i+1].strip()              # 192.168.122.xxx
            metrics_lines = cpu_blocks[i+2].strip().split('\n')
            row = {'Time': timestamp, 'type': cpu_type, 'ip': ip, 'target': target}
            for line in metrics_lines:
                line = line.strip()
                if not line or ':' not in line:
                    continue
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                try:
                    value = float(value) if '.' in value else int(value)
                except ValueError:
                    pass
                row[key] = value
                if key not in metric_order:
                    metric_order.append(key)
            all_rows.append(row)

    return all_rows, metric_order

def save_cpu_to_csv(rows, metric_names, output_path):
    fieldnames = ['Time', 'ip'] + metric_names + ['target']
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval='')
        writer.writeheader()
        for row in rows:
            filtered_row = {k: row.get(k, '') for k in fieldnames}
            writer.writerow(filtered_row)


def target_adjustment_cpuidle(filepath, output_path, threshold02=95, thereshold03=97):
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    start_time=None
    duration=20
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
        if row['ip']=='192.168.122.102' and float(row['cpu_idle']) > threshold02:
            row['target'] = 0
        elif row['ip']=='192.168.122.103' and float(row['cpu_idle']) > thereshold03:
            row['target'] = 0
        else:
            row['target'] = 0
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

# ===== 使用示例 =====
input_file = '/workspace/gpu_cluster/lyc/abnormal_data/cpu/fullload/3/cpu-1769854457.6318326'
output_file = '/workspace/gpu_cluster/data_processing/4090/cpu/cpu_metrics_with_label.csv'

rows, metric_names = parse_cpu_metrics_file(input_file)
save_cpu_to_csv(rows, metric_names, output_file)
target_adjustment_cpuidle(output_file, output_file, threshold02=95, thereshold03=97)

print(f"✅ 已解析 {len(rows)} 行 CPU 数据")
print(f"📊 涉及指标: {metric_names}")
print(f"💾 保存至: {output_file}")