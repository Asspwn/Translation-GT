import os
import json
import gzip
from tqdm import tqdm
import re
import openpyxl
from openpyxl import Workbook
import sys

# Define illegal characters pattern for Excel
ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

# Define replacement pattern for newline character
REPLACER = ('\n', r'\<>')  # Replace newline character with \<>

def clean_illegal_characters(value):
    if isinstance(value, dict) or isinstance(value, list):
        value = json.dumps(value)  # Convert dict or list to a JSON string
    if isinstance(value, str):
        value = value.replace(*REPLACER)  # Apply the replacement
        return ILLEGAL_CHARACTERS_RE.sub('', value)
    return value

def split_json_to_excel(input_file, output_folder, max_size_in_bytes, original_file_name):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    overhead_buffer = 0.90  # Adjust this value based on observed overhead
    adjusted_max_size_in_bytes = int(max_size_in_bytes * overhead_buffer)

    current_size = 0
    chunk_index = 1
    base_name = os.path.splitext(original_file_name)[0]
    current_file_name = os.path.join(output_folder, f'{base_name}_chunk_{chunk_index}.xlsx')
    current_wb = Workbook()
    current_ws = current_wb.active

    total_rows = sum(1 for _ in open(input_file, 'rt', encoding='utf-8'))  # Remove -1, no header assumption
    processed_rows = 0

    buffer = []

    with open(input_file, 'rt', encoding='utf-8') as jsonfile:
        for row in tqdm(jsonfile, total=total_rows, desc=f"Processing {original_file_name}", unit="row"):
            row = row.strip()  # Remove leading/trailing whitespace
            if not row:  # Skip empty lines
                continue
            try:
                row_data = json.loads(row)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON row at line {processed_rows + 1}")
                continue

            filtered_row = {key: clean_illegal_characters(value) for key, value in row_data.items()}
            filtered_row["original_index"] = processed_rows # Add original index column
            row_size = len(str(filtered_row))

            if processed_rows == 0:
                headers = list(filtered_row.keys())
                current_ws.append(headers)
                current_size += sum(len(str(cell)) for cell in headers)

            if current_size + row_size > adjusted_max_size_in_bytes:
                # Write the buffered rows to the current Excel file
                for buffered_row in buffer:
                    current_ws.append(list(buffered_row.values()))
                current_wb.save(current_file_name)

                # Start a new chunk
                chunk_index += 1
                current_file_name = os.path.join(output_folder, f'{base_name}_chunk_{chunk_index}.xlsx')
                current_wb = Workbook()
                current_ws = current_wb.active
                current_ws.append(headers)
                current_size = sum(len(str(cell)) for cell in headers) + row_size
                buffer = [filtered_row]
            else:
                buffer.append(filtered_row)
                current_size += row_size

            processed_rows += 1

        # Write remaining rows in buffer
        for buffered_row in buffer:
            current_ws.append(list(buffered_row.values()))

        current_wb.save(current_file_name)

    print(f"Finished processing {total_rows} rows into {chunk_index} files for {original_file_name}.")

def main():
    input_folder = '/home/aspandiyar/Get-data-huggingface/data/ucinlp-drop/data'
    output_folder_base = '/home/aspandiyar/Get-data-huggingface/data/ucinlp-drop/chunks'
    max_size_in_bytes = 4 * 1024 * 1024  # 4 MB, adjust if necessary

    json_files = [f for f in os.listdir(input_folder) if f.endswith('.jsonl')]

    for json_file in json_files:
        output_folder = os.path.join(output_folder_base, os.path.splitext(json_file)[0])
        
        # Check if the output folder already exists
        if os.path.exists(output_folder):
            print(f"Skipping {json_file} as output folder {output_folder} already exists.")
            continue
        
        file_path = os.path.join(input_folder, json_file)
        split_json_to_excel(file_path, output_folder, max_size_in_bytes, json_file)

if __name__ == "__main__":
    main()