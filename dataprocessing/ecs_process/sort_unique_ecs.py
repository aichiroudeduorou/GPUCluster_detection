import pandas as pd

# 文件路径
file_path = "/workspace/process_data_byBD/Data_alignment/tuomin_data/1.24/original_data/ecs_shiyan.csv"

# 读取 CSV（保持字符串类型避免精度丢失）
df = pd.read_csv(file_path, dtype=str)

# 删除完全重复的行（整行所有列都相同才视为重复），保留首次出现的
df = df.drop_duplicates(keep='first')

# 将 timestamp 转为数值用于排序（无效值转为 NaN）
df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')

# # 可选：删除 timestamp 无效的行（如果不需要可注释掉）
# df = df.dropna(subset=['timestamp'])

# 按 timestamp 升序排序
df = df.sort_values(by='timestamp', ascending=True)

# 将 timestamp 转回字符串（保持与原始格式一致，Int64 避免 .0）
df['timestamp'] = df['timestamp'].astype('Int64').astype(str)

# 保存回原文件（不带索引，保留原始列顺序和编码）
df.to_csv(file_path, index=False, encoding='utf-8-sig')

print(f"✅ 文件已去重 + 按 timestamp 排序并保存至: {file_path}")
print(f"📊 去重并清理后总行数: {len(df)}")
