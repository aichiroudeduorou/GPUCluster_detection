import pandas as pd
import os

input_path = '/workspace/gpu_cluster/lyc/abnormal_data/cpu/fullload/3/request feature-1769854461.csv'
output_path = '/workspace/gpu_cluster/data_processing/4090/cpu/request_metrics_label.csv'

# 读取数据
df = pd.read_csv(input_path)

# 新增target列，ttft>50为1，否则为0
# df['target'] = (df['ttft'] > 50).astype(int) # for burst
df['target'] = (df['ttft'] > 0.5).astype(int)

# 保存到新文件
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)
print(f"Labeled data saved to {output_path}")