import os
import sys
import shutil
import subprocess
import argparse
import re
import time
from utils.splitter import split_file_to_excel
from utils.merger import process_download_dir

def get_file_index(filename):
    """Extract numeric index from filename."""
    match = re.search(r'_(\d+)\.', filename)
    if match:
        return int(match.group(1))
    return float('inf')

def get_processed_files(output_dir):
    """Get set of already processed files based on translations."""
    processed = set()
    if os.path.exists(output_dir):
        for f in os.listdir(output_dir):
            # Extract original filename from translation filename
            # Example: convert "file_0-kk.jsonl" back to "file_0.jsonl"
            base = f.rsplit('-', 1)[0]
            if f.endswith('.json.gz'):
                original = f"{base}.json.gz"
            else:
                ext = os.path.splitext(f)[1]
                original = f"{base}{ext}"
            processed.add(original)
    return processed

def get_pending_files(folder_path, processed_files):
    """Get list of unprocessed files sorted by index."""
    all_files = [f for f in os.listdir(folder_path) 
                 if f.endswith(('.csv', '.jsonl', '.json.gz'))
                 and f not in processed_files]
    return sorted(all_files, key=get_file_index)

def recreate_directory(dir_path):
    """Remove and recreate a directory."""
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"Removed existing {os.path.basename(dir_path)} directory")
    os.makedirs(dir_path)
    print(f"Created fresh {os.path.basename(dir_path)} directory")

def recreate_working_directories():
    """Recreate both chunks and translated_chunks directories."""
    chunks_dir = os.path.join(os.getcwd(), 'chunks')
    translated_chunks_dir = os.path.join(os.getcwd(), 'translated_chunks')
    recreate_directory(chunks_dir)
    recreate_directory(translated_chunks_dir)

def process_single_file(input_file, lang):
    """Process a single file (CSV, JSONL, or gzipped JSON)."""
    output_folder_base = os.path.join(os.getcwd(), 'chunks')
    file_name = os.path.basename(input_file)
    if file_name.endswith('.json.gz'):
        output_folder = os.path.join(output_folder_base, os.path.splitext(os.path.splitext(file_name)[0])[0])
    else:
        output_folder = os.path.join(output_folder_base, os.path.splitext(file_name)[0])

    print(f"Splitting {file_name} into chunks...")
    split_file_to_excel(input_file, output_folder, 2 * 1024 * 1024, file_name)
    print(f"Chunks saved to {output_folder}")

def process_folder(folder_path, lang, keep_original_headers, check_interval=60):
    """
    Process files in a folder continuously, checking for new files periodically.
    
    Args:
        folder_path: Path to the folder containing files to process
        lang: Target language code
        keep_original_headers: Whether to keep original headers
        check_interval: Time in seconds between checks for new files
    """
    print(f"\nMonitoring folder: {folder_path}")
    
    relative_path = os.path.relpath(folder_path, args.folder)
    output_dir = os.path.join(os.getcwd(), 'translated', relative_path)
    os.makedirs(output_dir, exist_ok=True)

    while True:
        # Get set of already processed files
        processed_files = get_processed_files(output_dir)
        
        # Get list of pending files sorted by index
        pending_files = get_pending_files(folder_path, processed_files)
        
        if pending_files:
            print(f"\nFound {len(pending_files)} new files to process")
            
            for idx, input_file in enumerate(pending_files, 1):
                print(f"\nProcessing file {idx}/{len(pending_files)}: {input_file}")
                input_path = os.path.join(folder_path, input_file)
                
                # Skip if file is still being downloaded (size changing)
                initial_size = os.path.getsize(input_path)
                time.sleep(5)  # Wait a bit
                if initial_size != os.path.getsize(input_path):
                    print(f"File {input_file} is still being downloaded. Skipping for now.")
                    continue
                
                recreate_working_directories()
                process_single_file(input_path, lang)
                
                translate_script = os.path.join(os.getcwd(), 'utils', 'translate.py')
                subprocess.run([sys.executable, translate_script, "--target_language", lang])
                
                print(f"Merging translated chunks for {input_file}...")
                download_dir = os.path.join(os.getcwd(), 'translated_chunks')
                process_download_dir(download_dir, output_dir, '', lang, folder_path, keep_original_headers)
                
                print(f"Completed processing {input_file}")
        
        print(f"\nNo new files to process. Checking again in {check_interval} seconds...")
        time.sleep(check_interval)

def main():
    parser = argparse.ArgumentParser(description='Process and translate files continuously')
    parser.add_argument('folder', help='Root folder containing files to process')
    parser.add_argument('--lang', required=True, help='Target language code (e.g., kk for Kazakh)')
    parser.add_argument('--keep-headers', action='store_true', 
                      help='Keep original column headers instead of using translated ones')
    parser.add_argument('--check-interval', type=int, default=60,
                      help='Interval in seconds between checks for new files (default: 60)')
    global args
    args = parser.parse_args()

    if not os.path.exists(args.folder):
        print(f"Root folder not found: {args.folder}")
        return

    os.makedirs(os.path.join(os.getcwd(), 'translated'), exist_ok=True)

    try:
        if os.path.isfile(args.folder):
            print("Error: Please provide a folder path, not a file path")
            return
            
        # Process the folder continuously
        process_folder(args.folder, args.lang, args.keep_headers, args.check_interval)
            
    except KeyboardInterrupt:
        print("\nScript stopped by user")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")

if __name__ == "__main__":
    main()