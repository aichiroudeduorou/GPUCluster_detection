import os
import re
import csv

def check_duplicates():
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge.csv'
    if not os.path.exists(output_file):
        return

    print("\nChecking for duplicates...")
    seen = {}
    headers = None
    status_idx, inst_idx, ts_idx = -1, -1, -1

    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            for idx, row in enumerate(reader):
                # Skip empty rows
                if not row: continue
                
                # Identify header
                if 'status' in row and 'instance_id' in row and 'timestamp' in row:
                    # Update indices based on the current header (handling potential column shifts if any, 
                    # though generally they should be consistent if merged from same schema)
                    headers = row
                    status_idx = headers.index('status')
                    inst_idx = headers.index('instance_id')
                    ts_idx = headers.index('timestamp')
                    continue
                
                # If we haven't found a header yet, skip
                if headers is None:
                    continue
                    
                # Process data row
                try:
                    # Ensure indices are valid for this row
                    if len(row) > max(status_idx, inst_idx, ts_idx):
                        if row[status_idx].strip() == '0':
                            iid = row[inst_idx].strip()
                            ts = row[ts_idx].strip()
                            key = (iid, ts)
                            
                            if key not in seen:
                                seen[key] = []
                            seen[key].append(idx + 1) # 1-based line number
                except Exception:
                    continue

        # Report
        found = False
        for (iid, ts), indices in seen.items():
            if len(indices) > 1:
                found = True
                print(f"Row indices {indices}: instance_id={iid}, timestamp={ts}")
                
        if not found:
            print("No duplicates found.")
            
    except Exception as e:
        print(f"Error during duplicate check: {e}")

def check_context_consistency():
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge.csv'
    if not os.path.exists(output_file):
        print(f"File not found: {output_file}")
        return

    print("\nChecking context consistency...")
    
    rows = []
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                rows.append((i + 1, row)) # Store 1-based index and content
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    if not rows:
        print("File is empty.")
        return

    # Find initial headers
    status_idx, inst_idx = -1, -1
    
    for i, (ln, row) in enumerate(rows):
        if 'status' in row and 'instance_id' in row:
             status_idx = row.index('status')
             inst_idx = row.index('instance_id')
             break
    
    if status_idx == -1:
         print("No headers found.")
         return

    found_issues = False
    
    # Iterate
    for i, (ln, row) in enumerate(rows):
        # Skip if row is too short or empty
        if not row or len(row) <= max(status_idx, inst_idx):
            continue
            
        # Check if it's a header row itself (skip)
        if row[status_idx].strip() == 'status':
            continue
            
        if row[status_idx].strip() == '0':
            curr_iid = row[inst_idx].strip()
            
            # Check previous 10
            prev_ok = True
            if i < 10:
                prev_ok = False
            else:
                for k in range(1, 11):
                    neighbor_row = rows[i-k][1]
                    if not neighbor_row or len(neighbor_row) <= inst_idx:
                        prev_ok = False
                        break
                    if neighbor_row[inst_idx].strip() != curr_iid:
                        prev_ok = False
                        break
            
            # Check next 10
            next_ok = True
            if i + 10 >= len(rows):
                next_ok = False
            else:
                for k in range(1, 11):
                    neighbor_row = rows[i+k][1]
                    if not neighbor_row or len(neighbor_row) <= inst_idx:
                        next_ok = False
                        break
                    if neighbor_row[inst_idx].strip() != curr_iid:
                        next_ok = False
                        break
            
            if not (prev_ok and next_ok):
                print(f"Row {ln}: instance_id={curr_iid} (status=0) failed context check")
                found_issues = True

    if not found_issues:
        print("All status=0 rows passed context check.")

def process_duplicates():
    input_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge.csv'
    output_file='/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deduplicated.csv'
    if not os.path.exists(input_file):
        return

    print("\nProcessing duplicates cleanup...")
    
    rows = []
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            rows = list(reader)
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Identify duplicates
    seen = {}
    indices_map = {} # row_idx -> {col: idx}
    current_map = None
    
    for i, row in enumerate(rows):
        if not row: continue
        
        # Check for header
        if 'status' in row and 'instance_id' in row and 'timestamp' in row:
            current_map = {
                'status': row.index('status'),
                'instance_id': row.index('instance_id'),
                'timestamp': row.index('timestamp'),
                'ip': row.index('ip') if 'ip' in row else -1,
                'description': row.index('description') if 'description' in row else -1
            }
            indices_map[i] = 'HEADER' # Mark as header
            continue
        
        # If headers found, process row
        if current_map:
            indices_map[i] = current_map
            s_idx = current_map['status']
            i_idx = current_map['instance_id']
            t_idx = current_map['timestamp']
            
            if len(row) > max(s_idx, i_idx, t_idx):
                if row[s_idx].strip() == '0':
                    iid = row[i_idx].strip()
                    ts = row[t_idx].strip()
                    key = (iid, ts)
                    if key not in seen:
                        seen[key] = []
                    seen[key].append(i)

    rows_to_delete = set()

    for key, idx_list in seen.items():
        # Case 1: len is 2. Remove if IP is empty.
        if len(idx_list) == 2:
            for ridx in idx_list:
                cmap = indices_map.get(ridx)
                if not cmap or cmap == 'HEADER': continue
                
                ip_col = cmap['ip']
                ip_val = ""
                if ip_col != -1 and len(rows[ridx]) > ip_col:
                    ip_val = rows[ridx][ip_col].strip()
                
                if not ip_val:
                    # Delete this row context
                    start = max(0, ridx - 10)
                    end = min(len(rows), ridx + 11)
                    for k in range(start, end):
                        rows_to_delete.add(k)

        # Case 2: len > 2. Keep first with IP, merge desc, delete others.
        elif len(idx_list) > 2:
            kept_idx = -1
            
            # Find first non-empty IP
            for ridx in idx_list:
                cmap = indices_map.get(ridx)
                if not cmap or cmap == 'HEADER': continue
                
                ip_col = cmap['ip']
                ip_val = ""
                if ip_col != -1 and len(rows[ridx]) > ip_col:
                    ip_val = rows[ridx][ip_col].strip()
                
                if ip_val:
                    kept_idx = ridx
                    break
            
            if kept_idx == -1:
                kept_idx = idx_list[0] # Fallback
            
            # Merge descriptions
            kept_cmap = indices_map[kept_idx]
            desc_col = kept_cmap['description']
            
            if desc_col != -1:
                current_desc = ""
                if len(rows[kept_idx]) > desc_col:
                    current_desc = rows[kept_idx][desc_col].strip()
                
                for ridx in idx_list:
                    if ridx == kept_idx: continue
                    other_cmap = indices_map.get(ridx)
                    other_desc_col = other_cmap['description']
                    
                    if other_desc_col != -1 and len(rows[ridx]) > other_desc_col:
                        d_val = rows[ridx][other_desc_col].strip()
                        if d_val and d_val != current_desc and d_val not in current_desc:
                             current_desc += f"; {d_val}"
                
                # Update kept row description
                # Ensure row is large enough? typically yes if columns aligned
                if len(rows[kept_idx]) > desc_col:
                    rows[kept_idx][desc_col] = current_desc
            
            # Mark others for deletion
            for ridx in idx_list:
                if ridx == kept_idx: continue
                start = max(0, ridx - 10)
                end = min(len(rows), ridx + 11)
                for k in range(start, end):
                    rows_to_delete.add(k)
            
            # Unmark kept block (priority to keep)
            start_keep = max(0, kept_idx - 10)
            end_keep = min(len(rows), kept_idx + 11)
            for k in range(start_keep, end_keep):
                if k in rows_to_delete:
                    rows_to_delete.remove(k)

    # Write back
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            kept_count = 0
            for i, row in enumerate(rows):
                if i not in rows_to_delete:
                    writer.writerow(row)
                    kept_count += 1
        print(f"Cleanup finished. Rows remaining: {kept_count}")
        
    except Exception as e:
        print(f"Error writing file: {e}")


def filter_files():
    source_dir = '/workspace/lyc/zejun/1.29/the_same_id'
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge.csv'

    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # List all files in the source directory
    try:
        files = os.listdir(source_dir)
    except FileNotFoundError:
        print(f"Directory not found: {source_dir}")
        return

    saved_count = 0
    total_files = 0

    print(f"Scanning files in {source_dir}...")

    # Open the output file in write mode
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for file in files:
            filepath = os.path.join(source_dir, file)
            
            # Skip directories
            if not os.path.isfile(filepath):
                continue
                
            total_files += 1
            
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                
                # Check if file has enough lines (1 header + 21 data lines)
                if len(lines) < 22:
                     print(f"Skipped file (too short): {file}")
                     continue

                # Ignore header (first line)
                data_lines = lines[1:]
                
                blocks_found = 0
                consecutive_non_empty = 0
                
                # Check for ALL blocks of 21 consecutive non-empty lines
                for i, line in enumerate(data_lines):
                    # Check if line is not empty (ignoring whitespace and lines with only commas)
                    if line.strip() and not re.match(r'^[, \t\r\n]*$', line):
                        consecutive_non_empty += 1
                        if consecutive_non_empty >= 21:
                            # We found a block ending at index i
                            # The block is from index (i - 20) to i inclusive
                            start_idx = i - 20
                            end_idx = i + 1 # Slice end is exclusive
                            
                            block_lines = data_lines[start_idx:end_idx]
                            
                            # Merge content to the output file
                            outfile.writelines(block_lines)
                            
                            # Ensure there is a newline between files or blocks if missing
                            if block_lines and not block_lines[-1].endswith('\n'):
                                outfile.write('\n')
                            
                            blocks_found += 1
                            
                            # Reset counter to avoid overlapping blocks? 
                            # If "overlapping" blocks (e.g. lines 1-21, 2-22) are NOT desired, reset to 0.
                            # If we want distinct blocks (e.g. lines 1-21, 22-42), reset to 0.
                            # User likely implies distinct groups or just "non-empty" sequences.
                            # Assuming standard distinct blocks logic:
                            consecutive_non_empty = 0 
                    else:
                        consecutive_non_empty = 0
                
                if blocks_found > 0:
                    saved_count += 1
                else:
                    print(f"Skipped file (no consecutive 21 lines): {file}")
                    
            except Exception as e:
                print(f"Error processing {file}: {e}")

    print(f"Processing complete.")
    print(f"Total files scanned: {total_files}")
    print(f"Files merged into {output_file}: {saved_count}")

def check_empty_columns():
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deduplicated.csv'
    report_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/empty_columns.csv'
    
    if not os.path.exists(output_file):
        print(f"File not found: {output_file}")
        return

    print("\nChecking for empty columns...")
    
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return
                
            num_cols = len(headers)
            empty_counts = {col: 0 for col in headers}
            total_rows = 0
            
            for row in reader:
                if not row: continue
                total_rows += 1
                
                for i, val in enumerate(row):
                    if i < num_cols:
                        v = val.strip()
                        if not v or v == '[]' or v == 'Unknown':
                            empty_counts[headers[i]] += 1
            
            print(f"Total rows: {total_rows}")
            
            fully_empty_cols = []
            for col in headers:
                count = empty_counts[col]
                if count == total_rows:
                    fully_empty_cols.append(col)
            
            if fully_empty_cols:
                print(f"Found {len(fully_empty_cols)} completely empty columns:")
                for col in fully_empty_cols:
                    print(f"{col}: {empty_counts[col]}")
                
                # Save to CSV
                try:
                    with open(report_file, 'w', encoding='utf-8', newline='') as rf:
                        writer = csv.writer(rf)
                        writer.writerow(['column_name', 'empty_count'])
                        for col in fully_empty_cols:
                            writer.writerow([col, empty_counts[col]])
                    print(f"\nSaved empty columns list to: {report_file}")
                except Exception as e:
                    print(f"Error saving report: {e}")
            else:
                print("No completely empty columns found.")
                
            # Optional: Show partials if user wants "all empty columns" to imply "cols with any empty"
            # But "empty columns" usually means fully empty. 
            # If user meant "cols with missing values", I'll list those too briefly.
            print("\nColumns with missing values:")
            for col in headers:
                count = empty_counts[col]
                if count > 0 and count < total_rows:
                    print(f"{col}: {count}")

    except Exception as e:
        print(f"Error checking empty columns: {e}")

def delete_empty_columns():
    input_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deempty_columns.csv'
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deconstant_columns.csv'
    empty_cols_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/constant_columns_ignore_empty.csv'

    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return
    
    if not os.path.exists(empty_cols_file):
        print(f"Empty columns file not found: {empty_cols_file}")
        return

    print("\nDeleting empty columns based on empty_columns.csv...")

    # Read columns to delete
    cols_to_delete = set()
    try:
        with open(empty_cols_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None) # Skip header
            for row in reader:
                if row:
                    cols_to_delete.add(row[0].strip())
        print(f"Loaded {len(cols_to_delete)} columns to delete.")
    except Exception as e:
        print(f"Error reading empty columns file: {e}")
        return

    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as fin, \
             open(output_file, 'w', encoding='utf-8', newline='') as fout:
            
            reader = csv.reader(fin)
            writer = csv.writer(fout)
            
            headers = next(reader, None)
            if not headers:
                print("Input file is empty.")
                return
                
            # Determine indices to keep
            indices_to_keep = []
            headers_to_keep = []
            
            for i, h in enumerate(headers):
                if h not in cols_to_delete:
                    indices_to_keep.append(i)
                    headers_to_keep.append(h)
            
            writer.writerow(headers_to_keep)
            
            row_count = 0
            for row in reader:
                new_row = []
                for i in indices_to_keep:
                    if i < len(row):
                        new_row.append(row[i])
                    else:
                        new_row.append("")
                writer.writerow(new_row)
                row_count += 1
                
        print(f"Successfully processed {row_count} rows.")
        print(f"Removed {len(headers) - len(headers_to_keep)} columns.")
        print(f"Saved to {output_file}")

    except Exception as e:
        print(f"Error processing files: {e}")

def extract_non_empty_timestamps():
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge.csv'
    report_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/non_empty_timestamps.csv'
    
    if not os.path.exists(output_file):
        print(f"File not found: {output_file}")
        return

    print("\nExtracting rows with non-empty timestamps...")
    
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return
            
            if 'timestamps' not in headers:
                print("Column 'timestamps' not found.")
                print(f"Columns: {headers}")
                return

            ts_idx = headers.index('timestamps')
            found_count = 0
            
            with open(report_file, 'w', encoding='utf-8', newline='') as rf:
                writer = csv.writer(rf)
                writer.writerow(['original_line_number'] + headers)
                
                for idx, row in enumerate(reader):
                    if len(row) > ts_idx:
                        val = row[ts_idx].strip()
                        if val:
                            found_count += 1
                            line_num = idx + 2
                            if found_count <= 5:
                                print(f"Line {line_num}: {val}")
                            writer.writerow([line_num] + row)
            
            print(f"Found {found_count} rows. Saved to {report_file}")

    except Exception as e:
        print(f"Error: {e}")

def check_constant_columns():
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge.csv'
    if not os.path.exists(output_file):
        print(f"File not found: {output_file}")
        return

    print("\nChecking for constant columns...")
    
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return

            # Read first data row
            first_row = next(reader, None)
            if not first_row:
                print("No data rows found.")
                return
            
            num_cols = len(headers)
            ref_values = []
            for i in range(num_cols):
                val = first_row[i].strip() if i < len(first_row) else ''
                ref_values.append(val)
            
            is_constant = [True] * num_cols
            total_rows = 1
            
            for row in reader:
                total_rows += 1
                for i in range(num_cols):
                    if is_constant[i]:
                        val = row[i].strip() if i < len(row) else ''
                        if val != ref_values[i]:
                            is_constant[i] = False
            
            print(f"Total rows scanned: {total_rows}")
            
            constant_cols = []
            for i in range(num_cols):
                if is_constant[i]:
                    constant_cols.append((headers[i], ref_values[i]))
            
            if constant_cols:
                print(f"Found {len(constant_cols)} constant columns:")
                for col, val in constant_cols:
                    print(f"{col} (Value: '{val}')")
            else:
                print("No constant columns found.")

    except Exception as e:
        print(f"Error checking constant columns: {e}")

def check_constant_columns_ignore_empty():
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deempty_columns.csv'
    if not os.path.exists(output_file):
        print(f"File not found: {output_file}")
        return

    print("\nChecking for constant columns (ignoring empty values)...")
    
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return

            num_cols = len(headers)
            ref_values = [None] * num_cols # Store the first non-empty value found
            is_constant = [True] * num_cols
            
            total_rows = 0
            
            for row in reader:
                total_rows += 1
                for i in range(num_cols):
                    if not is_constant[i]:
                        continue
                        
                    val = row[i].strip() if i < len(row) else ''
                    if not val: # Ignore empty strings
                        continue
                        
                    if ref_values[i] is None:
                        ref_values[i] = val
                    elif val != ref_values[i]:
                        is_constant[i] = False
            
            print(f"Total rows scanned: {total_rows}")
            
            constant_cols = []
            for i in range(num_cols):
                if is_constant[i]:
                    val = ref_values[i] if ref_values[i] is not None else "<All Empty>"
                    constant_cols.append((headers[i], val))
            
            if constant_cols:
                print(f"Found {len(constant_cols)} constant columns (ignoring empty):")
                for col, val in constant_cols:
                    print(f"{col} (Value: '{val}')")
                
                # Save to CSV
                report_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/constant_columns_ignore_empty.csv'
                try:
                    with open(report_file, 'w', encoding='utf-8', newline='') as rf:
                        writer = csv.writer(rf)
                        writer.writerow(['column_name', 'constant_value'])
                        for col, val in constant_cols:
                            writer.writerow([col, val])
                    print(f"\nSaved constant columns list to: {report_file}")
                except Exception as e:
                    print(f"Error saving report: {e}")
            else:
                print("No constant columns found.")

    except Exception as e:
        print(f"Error checking constant columns: {e}")

def check_sparse_columns():
    input_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deconstant_columns.csv'
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    print("\nChecking for sparse columns (>= 80% empty)...")
    
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return
                
            num_cols = len(headers)
            empty_counts = {col: 0 for col in headers}
            total_rows = 0
            
            for row in reader:
                total_rows += 1
                for i, val in enumerate(row):
                    if i < num_cols:
                        v = val.strip()
                        # Consider empty string, '[]', and 'Unknown' as empty based on previous logic, 
                        # but user prompt said "empty values", usually implying standard empty strings.
                        # I will stick to what looks like "missing" data based on check_empty_columns context:
                        if not v or v == '[]' or v == 'Unknown':
                            empty_counts[headers[i]] += 1
            
            if total_rows == 0:
                print("No data rows found.")
                return

            print(f"Total rows: {total_rows}")
            threshold = 0.8
            sparse_cols = []
            
            for col in headers:
                ratio = empty_counts[col] / total_rows
                if ratio >= threshold:
                    sparse_cols.append((col, ratio))
            
            if sparse_cols:
                print(f"Found {len(sparse_cols)} sparse columns (>= {threshold*100}% empty):")
                for col, ratio in sparse_cols:
                    print(f"{col}: {ratio:.2%}")
            else:
                print(f"No columns found with >= {threshold*100}% empty values.")

    except Exception as e:
        print(f"Error checking sparse columns: {e}")

def delete_sparse_columns():
    input_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_deconstant_columns.csv'
    output_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_desparse.csv'
    
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    # Columns to keep even if sparse
    whitelist = {'description', 'diag_id', 'exception_cnt', 'kernel_version'}
    threshold = 0.8

    print(f"\nDeleting sparse columns (>= {threshold*100}% empty), excluding {whitelist}...")

    try:
        # First pass: calculate sparsity
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return
                
            num_cols = len(headers)
            empty_counts = {col: 0 for col in headers}
            total_rows = 0
            
            for row in reader:
                total_rows += 1
                for i, val in enumerate(row):
                    if i < num_cols:
                        v = val.strip()
                        if not v or v == '[]' or v == 'Unknown':
                            empty_counts[headers[i]] += 1
        
        if total_rows == 0:
            print("No data rows to process.")
            return

        # Identify columns to delete
        cols_to_delete = set()
        for col in headers:
            ratio = empty_counts[col] / total_rows
            if ratio >= threshold:
                if col not in whitelist:
                    cols_to_delete.add(col)
                else:
                    print(f"Keeping sparse column '{col}' (sparsity: {ratio:.2%}) due to whitelist.")
        
        print(f"Columns to delete: {sorted(list(cols_to_delete))}")

        # Second pass: write new file
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as fin, \
             open(output_file, 'w', encoding='utf-8', newline='') as fout:
            
            reader = csv.reader(fin)
            writer = csv.writer(fout)
            
            # Header
            headers = next(reader)
            indices_to_keep = []
            headers_to_keep = []
            
            for i, h in enumerate(headers):
                if h not in cols_to_delete:
                    indices_to_keep.append(i)
                    headers_to_keep.append(h)
            
            writer.writerow(headers_to_keep)
            
            # Data
            for row in reader:
                new_row = []
                for i in indices_to_keep:
                    if i < len(row):
                        new_row.append(row[i])
                    else:
                        new_row.append("")
                writer.writerow(new_row)
                
        print(f"Successfully saved cleaned data to {output_file}")
        print(f"Removed {len(cols_to_delete)} columns.")

    except Exception as e:
        print(f"Error deleting sparse columns: {e}")

def check_string_columns():
    input_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_desparse.csv'
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    print("\nChecking for string (non-numeric) columns...")
    
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            # Check for large field size
            csv.field_size_limit(10000000)
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return
            
            num_cols = len(headers)
            is_numeric = [True] * num_cols
            sample_values = [None] * num_cols
            has_data = [False] * num_cols # Track if column has ANY non-empty data
            
            row_count = 0
            for row in reader:
                row_count += 1
                for i, val in enumerate(row):
                    if i < num_cols:
                        v = val.strip()
                        # Treat [], Unknown, empty string as null/missing, not string data
                        if not v or v == '[]' or v == 'Unknown':
                            continue
                        
                        has_data[i] = True
                        
                        if sample_values[i] is None:
                            sample_values[i] = v
                            
                        if is_numeric[i]:
                            try:
                                float(v)
                            except ValueError:
                                is_numeric[i] = False
                                sample_values[i] = v # Keep the non-numeric value as sample
            
            string_cols = []
            for i in range(num_cols):
                # If column has data and is NOT confirmed numeric, it's a string column
                # If it has no data, it's ambiguous, but let's list it if it wasn't dropped earlier
                if has_data[i] and not is_numeric[i]:
                    string_cols.append((headers[i], sample_values[i]))
            
            if string_cols:
                print(f"Found {len(string_cols)} string/categorical columns:")
                for col, sample in string_cols:
                    print(f"{col} (Sample: '{sample}')")
            else:
                print("No string columns found.")

    except Exception as e:
        print(f"Error checking string columns: {e}")
def check_numeric_columns():
    input_file = '/workspace/gpu_cluster/data_processing/ecs_get/data/merge_desparse.csv'
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    print("\nChecking for non-string (numeric) columns...")
    
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            # Check for large field size
            csv.field_size_limit(10000000)
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print("File is empty.")
                return
            
            num_cols = len(headers)
            is_numeric = [True] * num_cols
            # We assume numeric unless proven otherwise by a non-numeric string value
            # Note: Empty values, [], Unknown are skipped as before
            
            row_count = 0
            for row in reader:
                row_count += 1
                for i, val in enumerate(row):
                    if i < num_cols:
                        v = val.strip()
                        if not v or v == '[]' or v == 'Unknown':
                            continue
                        
                        if is_numeric[i]:
                            try:
                                float(v)
                            except ValueError:
                                is_numeric[i] = False
                                # Once false, always false
            
            numeric_cols = []
            for i in range(num_cols):
                if is_numeric[i]:
                    numeric_cols.append(headers[i])
            
            print(f"Total rows scanned: {row_count}")
            print(f"Found {len(numeric_cols)} non-string/numeric columns:")
            for col in numeric_cols:
                print(col)

    except Exception as e:
        print(f"Error checking numeric columns: {e}")
if __name__ == "__main__":
    # 合并数据，取连续21行
    # filter_files()
    # 检查status为0，且instance_id和timestamp相同的行
    # check_duplicates()
    # 保留有ip的行，合并description
    # process_duplicates()
    # check_duplicates() # Verify results
    # 检查为空，[]，UNKNOWN的列 共计94
    # check_empty_columns()
    # 先删除空列，再删除constant的列
    # delete_empty_columns()
    # extract_non_empty_timestamps()
    # 查看constant的列，忽略空值 共计36
    # check_constant_columns_ignore_empty()
    # 查看80%为空的列
    # check_sparse_columns()
    # delete_sparse_columns()
    check_numeric_columns()
    # check_sparse_columns()
    # 删除80%为空的列，保留白名单内的列
    # delete_sparse_columns()
    # 查看字符串列
    check_string_columns()    