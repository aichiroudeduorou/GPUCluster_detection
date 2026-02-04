# GPUCluster_detection

## ECS 数据清洗与预处理脚本
/dataprocessing/ecs_process/process_ecs.py
/dataprocessing/ecs_process/sort_unique_ecs.py

对原始 ECS 故障数据执行两阶段清洗：  
1. **字段修正**：针对 `date == 20260113` 的记录，将 `ip` 值移至 `instance_id`，并将原 `ip` 置为空（因该日数据中 `instance_id` 缺失而 `ip` 可用）。  
2. **去重与排序**：  
   - 删除完全重复的行（保留首次出现）；  
   - 按 `timestamp` 升序排序；  
   - 保持 `timestamp` 为整数字符串格式（避免浮点 `.0`）。  

输入：`shiyan.csv`（含异常日期字段）  
输出：清洗并排序后的 `ecs_shiyan.csv`，可直接用于后续 GPU-ECS 对齐任务。


## GPU 与 ECS 数据对齐流水线
/dataprocessing/align_multiple align_multiple.py
将 GPU 监控数据（`t2_*_masked.csv`、`t3_masked.csv`）与 ECS 故障记录按 `instance_id` 和时间戳（10 分钟窗口内）进行对齐。  
- **核心功能**：  
  - 为非关键列自动添加前缀（例如 `temp` → `t2_temp`、`t3_temp`），避免列名冲突。  
  - 动态发现并合并所有输入文件中的列。  
  - 每个 `instance_id` 输出一个 CSV 文件，时间线按 `status` 标记（`-1`：故障前，`0`：故障时刻，`1`：故障后）。  

输入：清洗后的 ECS 故障数据 + GPU 日志  
输出：按实例对齐的时间序列数据，保存在 `/output/the_same_id/` 目录中