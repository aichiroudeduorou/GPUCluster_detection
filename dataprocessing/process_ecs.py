import pandas as pd
import io
import numpy as np

# --- 步骤 2: 加载数据 ---
# 使用 pandas.read_csv 从文本数据加载 DataFrame
# 在您的实际应用中，替换 io.StringIO(csv_data) 为您的文件名，如 'ecs_data.csv'
file_path = "/workspace/process_data_byBD/Data_alignment/tuomin_data/1.24/original_data/shiyan.csv"
ecs_df = pd.read_csv(file_path)

# 将空字符串的 instance_id 替换为 NaN 以便后续处理
ecs_df['instance_id'] = ecs_df['instance_id'].replace(r'^\s*$', np.nan, regex=True)


print("--- 原始ECS数据 (处理前) ---")
print(ecs_df[['date', 'ip', 'instance_id']].to_string())
print("\n" + "="*50 + "\n")


# --- 步骤 3: 执行清洗逻辑 ---
# 找到 'date' 列为 20260113 的所有行
# 这是实现您“使用说明”中第一步的核心代码
condition = ecs_df['date'] == 20260113

# 对于满足条件的行：
# 1. 将 'ip' 列的值复制到 'instance_id' 列
ecs_df.loc[condition, 'instance_id'] = ecs_df.loc[condition, 'ip']

# 2. 将 'ip' 列的值设置为 NULL (在 pandas 中用 None 或 np.nan 表示)
ecs_df.loc[condition, 'ip'] = None


print("--- 清洗后的ECS数据 (处理后) ---")
print("您可以看到 20260113 的数据已经被修正，而 20260114 的数据保持不变。\n")
# .to_string() 可以保证所有行都被打印出来
print(ecs_df[['date', 'ip', 'instance_id']].to_string())


# 现在，`ecs_df` 就是您完成清洗后，可以用于后续匹配的最终表格。
# 例如，您可以将它保存到新的CSV文件中：
ecs_df.to_csv('ecs_shiyan.csv', index=False)

