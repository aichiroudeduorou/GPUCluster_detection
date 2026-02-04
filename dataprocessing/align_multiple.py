import pandas as pd
import os
import glob
import time

# --- 1. 配置区域 ---
ECS_FILE_PATH = '/workspace/process_data_byBD/Data_alignment/tuomin_data/1.24/original_data/ecs_cleaned_data.csv'
GPU_DATA_DIR = '/workspace/process_data_byBD/Data_alignment/tuomin_data/1.24/original_data/'
OUTPUT_DIR = '/workspace/process_data_byBD/Data_alignment/tuomin_data/1.24/output/the_same_id/' # 使用新的输出目录
TIME_WINDOW_SECONDS = 10 * 60
CHUNK_SIZE = 500000

# 定义不应被重命名的关键列
KEY_COLUMNS = {'instance_id', 'ip', 'timestamp', 'device_name'}

# --- 2. 核心处理函数 ---

def build_fault_index(ecs_df):
    """
    从ECS DataFrame构建一个故障索引字典。
    """
    print("开始构建故障索引...")
    ecs_df['timestamp'] = pd.to_numeric(ecs_df['timestamp'], errors='coerce')
    ecs_df.dropna(subset=['timestamp'], inplace=True)
    ecs_df['timestamp'] = ecs_df['timestamp'].astype(int)
    
    # # 仅当 instance_id, timestamp, diag_id 三者都相同时才去重
    # ecs_df.drop_duplicates(subset=['instance_id', 'timestamp', 'diag_id'], inplace=True)
    # print(f"去重后，剩余 {len(ecs_df)} 条有效故障记录。")

    # 如果去重逻辑被注释，这条日志会显示原始记录数
    print(f"ecs_df 中共有 {len(ecs_df)} 条故障记录待处理。")

    faults_index = {}
    for _, row in ecs_df.iterrows():
        instance_id = row['instance_id']
        if instance_id not in faults_index:
            faults_index[instance_id] = []
        faults_index[instance_id].append((row['timestamp'], row.get('ip'), row))
        
    print("故障索引构建完成。")
    return faults_index

# --- **已重构以支持列重命名和动态列发现** ---
def process_gpu_files(gpu_file_paths, faults_index, initial_all_columns_set):
    """
    流式处理GPU文件，在合并前重命名冲突列，并动态发现所有列。
    """
    print("\n开始处理GPU数据文件并合并行...")
    
    # matched_data 结构不变
    matched_data = {
        instance_id: {
            fault[0]: {} for fault in faults
        } for instance_id, faults in faults_index.items()
    }
    
    instance_ids_to_find = set(faults_index.keys())
    # 使用传入的集合来动态收集所有列名
    all_columns = initial_all_columns_set.copy()

    for file_path in gpu_file_paths:
        print(f"  正在处理文件: {os.path.basename(file_path)}")
        
        # --- **新的逻辑：确定列前缀** ---
        filename = os.path.basename(file_path)
        prefix = None
        if filename.startswith('t2_'):
            prefix = 't2'
        elif filename.startswith('t3_'): # 假设t3文件名是 t3_masked.csv
            prefix = 't3'

        try:
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=False):
                # --- **新的逻辑：重命名列** ---
                if prefix:
                    cols_to_rename = [col for col in chunk.columns if col not in KEY_COLUMNS]
                    rename_dict = {col: f"{prefix}_{col}" for col in cols_to_rename}
                    chunk.rename(columns=rename_dict, inplace=True)
                
                # 更新全局列集合
                all_columns.update(chunk.columns)

                # 预处理数据类型 (与之前相同)
                chunk['timestamp'] = pd.to_numeric(chunk['timestamp'], errors='coerce')
                chunk.dropna(subset=['timestamp', 'instance_id'], inplace=True)
                chunk['timestamp'] = chunk['timestamp'].astype(int)
                
                relevant_chunk = chunk[chunk['instance_id'].isin(instance_ids_to_find)]
                if relevant_chunk.empty:
                    continue

                for _, gpu_row in relevant_chunk.iterrows():
                    gpu_instance_id = gpu_row['instance_id']
                    gpu_ts = gpu_row['timestamp']
                    gpu_ip = gpu_row.get('ip')
                    
                    for fault_ts, fault_ip, _ in faults_index[gpu_instance_id]:
                        if abs(gpu_ts - fault_ts) <= TIME_WINDOW_SECONDS:
                            ip_match = (pd.isna(fault_ip) or str(fault_ip).strip() == '') or (fault_ip == gpu_ip)

                            if ip_match:
                                # 因为列名已经被重命名，现在 update 会安全地添加新列
                                # 例如：先添加 t2_temp，后添加 t3_temp，两者都会保留
                                existing_record = matched_data[gpu_instance_id][fault_ts].get(gpu_ts)
                                if existing_record:
                                    existing_record.update(gpu_row.to_dict())
                                else:
                                    matched_data[gpu_instance_id][fault_ts][gpu_ts] = gpu_row.to_dict()
        except Exception as e:
            print(f"    处理文件 {file_path} 时发生错误: {e}")
            continue

    print("GPU数据文件处理完成。")
    # 返回匹配的数据和所有动态发现的列的集合
    return matched_data, all_columns


def generate_output_files(matched_data, faults_index, output_dir, all_discovered_columns_set):
    """
    根据匹配并合并后的数据，为每个instance_id生成一个CSV文件。
    """
    print("\n开始生成输出文件...")
    os.makedirs(output_dir, exist_ok=True)
    
    # --- **新的逻辑：对动态发现的列进行排序** ---
    # 将集合转换为列表并排序，以获得一致的列顺序
    # 将关键列放在前面，其他列按字母排序
    sorted_cols = sorted(list(all_discovered_columns_set))
    # 使用 set 而不是 list for KEY_COLUMNS for faster lookups
    ordered_key_cols = [col for col in ['instance_id', 'ip', 'timestamp', 'device_name'] if col in sorted_cols]
    other_cols = [col for col in sorted_cols if col not in ordered_key_cols]
    final_ordered_cols = ['status'] + ordered_key_cols + other_cols

    # 后续逻辑与之前基本相同，但使用新的列顺序
    for instance_id, faults in faults_index.items():
        # ... (此部分代码与原版相同) ...
        sorted_faults = sorted(faults, key=lambda x: x[0])
        all_blocks_for_instance = []
        
        for fault_ts, _, ecs_row in sorted_faults:
            merged_gpu_rows_dict = matched_data[instance_id].get(fault_ts, {})
            
            ecs_df_row = ecs_row.to_frame().T
            ecs_df_row['status'] = 0
            
            if merged_gpu_rows_dict:
                gpu_df = pd.DataFrame(list(merged_gpu_rows_dict.values()))
                gpu_df['status'] = gpu_df['timestamp'].apply(lambda ts: -1 if ts < fault_ts else 1)
                combined_df = pd.concat([gpu_df, ecs_df_row], ignore_index=True)
            else:
                combined_df = ecs_df_row

            # 使用新的、完整的列顺序来重新索引
            combined_df = combined_df.reindex(columns=final_ordered_cols)
            combined_df.sort_values(by='timestamp', inplace=True, ascending=True)

            all_blocks_for_instance.append(combined_df)

        if not all_blocks_for_instance:
            continue

        final_df_for_instance = pd.DataFrame(columns=final_ordered_cols)
        for i, block in enumerate(all_blocks_for_instance):
            final_df_for_instance = pd.concat([final_df_for_instance, block], ignore_index=True)
            if i < len(all_blocks_for_instance) - 1:
                empty_row = pd.DataFrame([{}], columns=final_ordered_cols)
                final_df_for_instance = pd.concat([final_df_for_instance, empty_row], ignore_index=True)
                
        output_path = os.path.join(output_dir, f"{instance_id}.csv")
        final_df_for_instance.to_csv(output_path, index=False)
        print(f"  已生成文件: {output_path}")

    print("所有输出文件已生成完毕。")


# --- 3. 主执行逻辑 (已调整) ---
def main():
    start_time = time.time()
    
    try:
        ecs_df = pd.read_csv(ECS_FILE_PATH, low_memory=False)
    except FileNotFoundError:
        print(f"错误：找不到ECS文件 '{ECS_FILE_PATH}'。请检查路径。")
        return
        
    faults_index = build_fault_index(ecs_df)
    
    if not faults_index:
        print("没有有效的故障数据可供处理。")
        return

    # --- **新的逻辑：准备文件路径和初始列集合** ---
    # 初始化列集合，首先包含ECS文件的所有列
    initial_columns_set = set(ecs_df.columns)

    t2_files = glob.glob(os.path.join(GPU_DATA_DIR, 't2_*_masked.csv'))
    t3_file = os.path.join(GPU_DATA_DIR, 't3_masked.csv')
    gpu_file_paths = sorted(t2_files)
    if os.path.exists(t3_file):
        # 确保 t3 文件在 t2 文件之后处理，以防万一
        gpu_file_paths.append(t3_file)

    if not gpu_file_paths:
        print(f"错误：在 '{GPU_DATA_DIR}' 中找不到GPU数据文件。")
        return
        
    # **旧的 discover_all_columns 函数已被移除**
    
    # 调用重构后的核心函数，它会返回匹配数据和所有列的集合
    matched_data, all_columns_set = process_gpu_files(gpu_file_paths, faults_index, initial_columns_set)
    
    print(f"\n动态发现完成。共发现 {len(all_columns_set)} 个唯一的列。")

    # 将最终的列集合传递给输出函数
    generate_output_files(matched_data, faults_index, OUTPUT_DIR, all_columns_set)

    end_time = time.time()
    print(f"\n任务完成！总耗时: {end_time - start_time:.2f} 秒。")

if __name__ == '__main__':
    main()

