import os
import json
import csv
import re
import sys
import gzip
from openpyxl import Workbook
from tqdm import tqdm
import io
from concurrent.futures import ThreadPoolExecutor
from functools import partial

# Increase field size limit to avoid _csv.Error
csv.field_size_limit(sys.maxsize)

# Define illegal characters pattern for Excel
ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\037\177]')
REPLACER = ('\n', r'\<>')

def clean_illegal_characters(value):
    """Optimized cleaning function with type checking"""
    if not value:  # Handle None or empty values quickly
        return value
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    if isinstance(value, str):
        if '\n' in value:  # Only replace if newline exists
            value = value.replace(*REPLACER)
        return ILLEGAL_CHARACTERS_RE.sub('', value)
    return value

def detect_file_format(file_path):
    """Detect if the file is CSV, JSONL, or gzipped JSON based on extension and content."""
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

def save_chunk(chunk_data, headers, output_file):
    """Save a chunk of data to Excel file"""
    wb = Workbook(write_only=True)  # Use write_only mode for better performance
    ws = wb.create_sheet()
    
    ws.append(headers)
    for row in chunk_data:
        ws.append([row.get(header, '') for header in headers])
    
    wb.save(output_file)

def process_chunk(chunk_data, headers, output_file):
    """Process and save a chunk of data"""
    try:
        save_chunk(chunk_data, headers, output_file)
        return True
    except Exception as e:
        print(f"Error saving chunk {output_file}: {e}")
        return False

def process_file_to_excel(input_file, output_folder, max_size_in_bytes, original_file_name, file_format):
    """Unified processing function for all file types"""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    overhead_buffer = 0.7
    adjusted_max_size_in_bytes = int(max_size_in_bytes * overhead_buffer)
    base_name = os.path.splitext(original_file_name)[0]
    
    # Initialize variables
    current_size = 0
    chunk_index = 1
    buffer = []
    processed_rows = 0
    chunk_sizes = []  # Track chunk sizes for better size estimation
    
    # Function to get file opener based on format
    if file_format == 'gzjson':
        file_opener = lambda f: gzip.open(f, 'rt', encoding='utf-8')
        base_name = os.path.splitext(base_name)[0]  # Remove both extensions
    else:
        file_opener = lambda f: open(f, 'r', encoding='utf-8')

    # Read headers
    with file_opener(input_file) as f:
        if file_format in ['jsonl', 'gzjson']:
            first_line = f.readline().strip()
            headers = list(json.loads(first_line).keys()) + ["original_index"]
        else:  # CSV
            reader = csv.DictReader(f)
            headers = reader.fieldnames + ["original_index"]

    # Initialize ThreadPoolExecutor for parallel chunk saving
    with ThreadPoolExecutor(max_workers=min(os.cpu_count(), 4)) as executor:
        futures = []
        
        # Process file in chunks
        with file_opener(input_file) as f:
            # Skip header for JSONL/GZJSON as we already read it
            if file_format in ['jsonl', 'gzjson']:
                f.readline()

            # Create reader based on file format
            if file_format == 'csv':
                reader = csv.DictReader(f)
            else:
                reader = (json.loads(line.strip()) for line in f)

            # Process rows with progress bar
            for row in tqdm(reader, desc=f"Processing {original_file_name}", unit="row"):
                filtered_row = {
                    header: clean_illegal_characters(row.get(header, ''))
                    for header in headers[:-1]  # Exclude original_index
                }
                filtered_row["original_index"] = processed_rows

                row_size = sum(len(str(val)) for val in filtered_row.values())

                if current_size + row_size > adjusted_max_size_in_bytes and buffer:
                    # Save current buffer
                    output_file = os.path.join(output_folder, f'{base_name}_chunk_{chunk_index}.xlsx')
                    
                    # Submit chunk processing to thread pool
                    future = executor.submit(process_chunk, buffer.copy(), headers, output_file)
                    futures.append(future)
                    
                    # Track chunk size for better estimation
                    chunk_sizes.append(current_size)
                    
                    # Reset for next chunk
                    chunk_index += 1
                    current_size = row_size
                    buffer = [filtered_row]
                else:
                    buffer.append(filtered_row)
                    current_size += row_size

                processed_rows += 1

            # Save remaining buffer
            if buffer:
                output_file = os.path.join(output_folder, f'{base_name}_chunk_{chunk_index}.xlsx')
                future = executor.submit(process_chunk, buffer, headers, output_file)
                futures.append(future)
                chunk_sizes.append(current_size)

        # Wait for all chunks to be saved
        for future in futures:
            future.result()  # This will raise any exceptions that occurred

    # Calculate and print statistics
    avg_chunk_size = sum(chunk_sizes) / len(chunk_sizes) if chunk_sizes else 0
    print(f"\nProcessing Statistics:")
    print(f"Total rows processed: {processed_rows}")
    print(f"Total chunks created: {chunk_index}")
    print(f"Average chunk size: {avg_chunk_size / 1024:.2f} KB")
    print(f"Finished processing {original_file_name}")

def split_file_to_excel(input_file, output_folder, max_size_in_bytes, original_file_name):
    """Main function to handle all file types"""
    file_format = detect_file_format(input_file)
    process_file_to_excel(input_file, output_folder, max_size_in_bytes, original_file_name, file_format)

def main():
    """Main entry point for the script."""
    output_folder_base = os.path.join(os.getcwd(), 'chunks')
    max_size_in_bytes = 2 * 1024 * 1024  # 2 MB, adjust if necessary

    # Get input file path (assuming it's passed as an argument or environment variable)
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = os.environ.get('INPUT_FILE')
        if not input_file:
            print("Error: No input file specified")
            sys.exit(1)

    file_name = os.path.basename(input_file)
    output_folder = os.path.join(output_folder_base, os.path.splitext(file_name)[0])

    # Check if the output folder already exists
    if os.path.exists(output_folder):
        print(f"Skipping {file_name} as output folder {output_folder} already exists.")
        return

    print(f"\nProcessing {file_name}...")
    try:
        split_file_to_excel(input_file, output_folder, max_size_in_bytes, file_name)
        print(f"Successfully processed {file_name}. Output saved to {output_folder}")
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()