import pandas as pd
import os
import re
import json
import gzip

REPLACER = (r'\<>', '\n')

def detect_original_format(file_path):
    """Detect if the original file is CSV, JSONL, or gzipped JSON."""
    base_ext = os.path.splitext(file_path)[1].lower()
    if base_ext == '.gz':
        # Get the extension before .gz
        root_ext = os.path.splitext(os.path.splitext(file_path)[0])[1].lower()
        if root_ext in ['.json', '.jsonl']:
            return 'gzjson'
    elif base_ext == '.csv':
        return 'csv'
    elif base_ext == '.jsonl':
        return 'jsonl'
    else:
        raise ValueError(f"Unsupported file format: {base_ext}")

def merge_to_jsonl(df, output_file):
    """Convert DataFrame to JSONL format and save."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            json_record = row.dropna().to_dict()
            # Remove the original_index if it exists
            json_record.pop('original_index', None)
            f.write(json.dumps(json_record, ensure_ascii=False) + '\n')

def merge_to_gzjson(df, output_file):
    """Convert DataFrame to gzipped JSON format and save."""
    with gzip.open(output_file, 'wt', encoding='utf-8') as f:
        for _, row in df.iterrows():
            json_record = row.dropna().to_dict()
            # Remove the original_index if it exists
            json_record.pop('original_index', None)
            f.write(json.dumps(json_record, ensure_ascii=False) + '\n')

def merge_files(base_filename, download_dir, output_dir, output_lang, original_file_path=None, keep_original_headers=False):
    """Merge translated Excel chunks with protected column handling."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get original column structure
    original_columns = None
    if original_file_path and os.path.exists(original_file_path):
        try:
            original_format = detect_original_format(original_file_path)
            if original_format == 'csv':
                original_df = pd.read_csv(original_file_path, nrows=1)  # Read just first row for headers
                original_columns = list(original_df.columns)
            elif original_format in ['jsonl', 'gzjson']:
                opener = gzip.open if original_format == 'gzjson' else open
                with opener(original_file_path, 'rt', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    original_columns = list(json.loads(first_line).keys())
            print(f"Original columns detected: {original_columns}")
        except Exception as e:
            print(f"Error reading original headers: {e}")
            original_columns = None

    # Find and sort chunks
    pattern = re.compile(fr'^{re.escape(base_filename)}_chunk_(\d+)\.xlsx$')
    files_to_merge = []
    for filename in os.listdir(download_dir):
        match = pattern.match(filename)
        if match:
            chunk_number = int(match.group(1))
            files_to_merge.append((chunk_number, filename))

    if not files_to_merge:
        print("No files found to merge")
        return

    files_to_merge.sort(key=lambda x: x[0])
    merged_data = []

    # Process first chunk to establish column structure
    first_chunk_path = os.path.join(download_dir, files_to_merge[0][1])
    try:
        first_df = pd.read_excel(first_chunk_path)
        
        # Clean up column names - remove any concatenated or malformed headers
        clean_columns = []
        for col in first_df.columns:
            # Split on common concatenation patterns
            parts = str(col).split('**')
            clean_col = parts[0].strip()
            clean_columns.append(clean_col)
        
        first_df.columns = clean_columns
        
        # Use original columns if available and requested
        if keep_original_headers and original_columns:
            # Create mapping between translated and original columns
            col_mapping = {}
            for i, col in enumerate(clean_columns):
                if i < len(original_columns):
                    col_mapping[col] = original_columns[i]
            
            # Rename columns using the mapping
            first_df = first_df.rename(columns=col_mapping)
            reference_columns = original_columns
        else:
            reference_columns = clean_columns

        # Add to merged data
        if 'original_index' in first_df.columns:
            reference_columns = [col for col in reference_columns if col != 'original_index'] + ['original_index']
        
        merged_data.append(first_df[reference_columns])
        
    except Exception as e:
        print(f"Error processing first chunk: {e}")
        return

    # Process remaining chunks
    for chunk_number, filename in files_to_merge[1:]:
        file_path = os.path.join(download_dir, filename)
        print(f"Reading file: {file_path}")

        try:
            df = pd.read_excel(file_path)
            
            # Clean column names
            df.columns = [str(col).split('**')[0].strip() for col in df.columns]
            
            # Ensure column alignment
            if len(df.columns) >= len(reference_columns):
                df = df.iloc[:, :len(reference_columns)]
                df.columns = reference_columns
            else:
                print(f"Warning: Chunk {chunk_number} has fewer columns than reference")
                continue

            # Replace newlines in text fields
            df = df.astype(str).map(lambda x: x.replace(*REPLACER))
            merged_data.append(df[reference_columns])

        except Exception as e:
            print(f"Error reading chunk {chunk_number}: {e}")
            continue

    if merged_data:
        try:
            final_df = pd.concat(merged_data, ignore_index=True)
            
            # Remove original_index before saving if it exists
            if 'original_index' in final_df.columns:
                final_df = final_df.drop('original_index', axis=1)

            # Save in original format
            original_format = detect_original_format(original_file_path)
            base_name = os.path.splitext(os.path.basename(original_file_path))[0]
            
            if original_format == 'gzjson':
                base_name = os.path.splitext(base_name)[0]
                output_file = os.path.join(output_dir, f'{base_name}-{output_lang}.json.gz')
                merge_to_gzjson(final_df, output_file)
            elif original_format == 'jsonl':
                output_file = os.path.join(output_dir, f'{base_name}-{output_lang}.jsonl')
                merge_to_jsonl(final_df, output_file)
            else:  # csv
                output_file = os.path.join(output_dir, f'{base_name}-{output_lang}.csv')
                final_df.to_csv(output_file, index=False, encoding='utf-8')
                
            print(f"All files merged successfully into {output_file}.")
        except Exception as e:
            print(f"Error during final merge and save: {e}")
    else:
        print("No data to merge")
        
def process_download_dir(download_dir, output_dir, starts_with, output_lang, original_folder, keep_original_headers=False):
    """Process downloaded chunks directory."""
    if not os.path.exists(original_folder):
        print(f"Original folder not found: {original_folder}")
        return

    # Get list of original files (CSV, JSONL, and gzipped JSON)
    original_files = {f for f in os.listdir(original_folder) 
                     if f.endswith(('.csv', '.jsonl', '.json.gz'))}
    
    # Process only files that exist in original folder
    for original_file in original_files:
        # For gzipped files, use the name without both extensions
        if original_file.endswith('.json.gz'):
            base_filename = os.path.splitext(os.path.splitext(original_file)[0])[0]
        else:
            base_filename = os.path.splitext(original_file)[0]
            
        original_file_path = os.path.join(original_folder, original_file)
        
        print(f"Processing translations for: {original_file}")
        merge_files(base_filename, download_dir, output_dir, output_lang, original_file_path, keep_original_headers)