import pandas as pd
import os
import re
import json
import gzip

# Define replacement pattern
REPLACER = (r'\<>', '\n')  # Replace \<> with a newline character

def merge_files(base_filename, download_dir, output_dir):
    """
    Merge Excel files with the same base filename into a single Hugging Face datasets-compatible JSON.gz file, ensuring they are merged in order by chunk number.
    The output file will ensure all values are correctly cast as strings.
    """
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist. Creating it.")
        os.makedirs(output_dir)

    # Output file - ensuring it is in the correct Hugging Face datasets format
    output_file = os.path.join(output_dir, f'{base_filename}-{output_lang}.json.gz')

    # Column names for the merged file
    column_names = ['texts', 'original_index']

    # Define the pattern to match filenames based on the provided base_filename
    pattern = re.compile(fr'^{re.escape(base_filename)}_chunk_(\d+)\.xlsx$')

    # Track if any files were processed
    files_processed = False

    # List to store filenames and their corresponding chunk numbers
    files_to_merge = []

    # Process each file in the download directory
    for filename in os.listdir(download_dir):
        match = pattern.match(filename)
        if match:
            chunk_number = int(match.group(1))  # Extract the chunk number
            files_to_merge.append((chunk_number, filename))

    # Sort files by chunk number
    files_to_merge.sort(key=lambda x: x[0])

    # List to hold all merged data (rows)
    merged_data = []

    # Merge the files in order
    for chunk_number, filename in files_to_merge:
        file_path = os.path.join(download_dir, filename)
        print(f"Reading file: {file_path}")

        try:
            # Read the file into a dataframe
            df = pd.read_excel(file_path, header=None, skiprows=1)

            # Rename columns to match the specified column names
            if len(df.columns) <= len(column_names):
                df.columns = column_names[:len(df.columns)]
            else:
                # Handle case where there are more columns than expected
                df.columns = column_names + [f'Extra_{i}' for i in range(len(df.columns) - len(column_names))]

            # Cast all columns to string to avoid pyarrow data type issues
            df = df.astype(str).applymap(lambda x: x.replace(*REPLACER))

            # Convert the dataframe to a list of dictionaries and add to merged data
            merged_data.extend(df.to_dict(orient='records'))

            files_processed = True
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")

    if not files_processed:
        print("No files were processed. Please check the file naming or directory contents.")
    else:
        # Write the merged data to a JSON.gz file with ascii=False, ensuring it's Hugging Face-compatible
        with gzip.open(output_file, 'wt', encoding='utf-8') as gz_file:
            for record in merged_data:
                gz_file.write(json.dumps(record, ensure_ascii=False) + "\n")  # Each record must be on a new line
        print(f"All files merged successfully into {output_file}.")

def process_download_dir(download_dir, output_dir, starts_with):
    """
    Iterate through all files in the download directory, identify unique base filenames, and merge them by their chunk numbers.

    :param download_dir: The directory where the Excel files are stored.
    :param output_dir: The directory where the merged JSON.gz files will be saved.
    :param starts_with: The prefix that filenames should start with to be processed.
    """
    # Set to track base filenames
    unique_filenames = set()

    # Regular expression to identify base filenames and chunks
    base_filename_pattern = re.compile(rf'^({re.escape(starts_with)}.*)_chunk_\d+\.xlsx$')

    # Identify unique base filenames
    for filename in os.listdir(download_dir):
        match = base_filename_pattern.match(filename)
        if match:
            base_filename = match.group(1)
            unique_filenames.add(base_filename)

    # Merge files for each unique base filename
    for base_filename in unique_filenames:
        print(f"Merging files for base filename: {base_filename}")
        merge_files(base_filename, download_dir, output_dir)

# Usage
download_dir = '/home/aspandiyar/Downloads'
output_dir = '/home/aspandiyar/Get-data-huggingface/data/the_cauldron/translated'
starts_with = ''
output_lang = 'kk'

# Process the download directory and merge files
process_download_dir(download_dir, output_dir, starts_with)
