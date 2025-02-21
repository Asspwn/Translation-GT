# CSV Translation Automation Tool

This tool automates the process of translating large data files using Google Translate. It's designed to handle large datasets by splitting them into manageable chunks, translating them, and then merging them back together while maintaining data integrity.

## Table of Contents
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Directory Structure](#directory-structure)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Troubleshooting](#troubleshooting)
- [Known Limitations](#known-limitations)

## Features

- Automated translation using Google Translate
- Handles large files by splitting them into smaller chunks
- Supports multiple file formats (CSV, JSONL, gzipped JSON)
- Supports nested folder structures
- Maintains original data structure and index ordering
- Preserves special characters and formatting
- Optional preservation of original column headers
- Progress tracking and error logging
- Automatic retry mechanism for failed translations
- Support for multiple target languages
- Resumes interrupted operations
- Parallel processing with 50 concurrent translators
- Maintains original file format in output

## Project Structure

```
project_root/
├── app.py              # Main application script
├── requirements.txt    # Python dependencies
├── README.md          # Documentation
└── utils/             # Utility modules
    ├── __init__.py    # Package initializer
    ├── merger.py      # Handles merging translated chunks
    ├── splitter.py    # Splits files into manageable chunks
    └── translate.py   # Manages Google Translate operations
```

## Prerequisites

- Python 3.7 or higher
- Google Chrome browser
- Stable internet connection
- Sufficient disk space for temporary files

## Prerequisites for Windows Users
Before installing the requirements, ensure you have the necessary build tools:
1. Download and install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
2. During installation, make sure to select "Desktop development with C++"
3. After installation, proceed with `pip install -r requirements.txt`

Note: This step is only necessary for Windows users. Linux and MacOS users can proceed directly to installing requirements.

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Verify Chrome installation:
```bash
google-chrome --version  # On Windows: check through Settings > About Chrome
```

## Usage

### Basic Usage

Run the translation process with:
```bash
python app.py <input_path> --lang <target_language_code>
```

Example:
```bash
# For a directory
python app.py ./data --lang kk

# For a single file
python app.py ./data/myfile.csv --lang kk
```

### Command Line Arguments

- `input_path`: Path to file or folder containing data files
- `--lang`: Target language code (e.g., "kk" for Kazakh, "ru" for Russian)
- `--keep-headers`: (Optional) Keep original column headers untranslated

### Examples

1. Translate to Kazakh, keeping original headers:
```bash
python app.py ./data --lang kk --keep-headers
```

2. Translate nested folder structure to Russian:
```bash
python app.py ./nested/data/folder --lang ru
```

## Directory Structure

The tool creates and manages several directories:

```
working_directory/
├── chunks/             # Temporary storage for split files
├── translated_chunks/  # Temporary storage for translated files
└── translated/        # Final output directory
    └── [maintains original folder structure]
```

### Input Requirements
- Files should be UTF-8 encoded
- Supported formats: CSV, JSONL, gzipped JSON
- No size limitation (files are automatically split)
- Headers should be in the first row
- Valid file format required

### Output Structure
- Maintains original folder hierarchy
- Files named as: `original_name-{lang_code}.<original_extension>`
- Preserves original column order and file format
- UTF-8 encoded output

## Configuration

### Chunk Size
Default chunk size is 2MB. Modify in app.py:
```python
split_file_to_excel(input_file, output_folder, 2 * 1024 * 1024, file_name)
```

### Translation Settings
Configure in utils/translate.py:
- Retry attempts
- Timeout values
- Browser settings
- Process pool size (default: 50)

## Error Handling

The tool implements several error handling mechanisms:

1. Failed Translations Log:
   - Located at: `translated_chunks/failed_chunks.txt`
   - Records files that failed to translate
   - Includes error messages and timestamps

2. Automatic Retries:
   - Failed translations are automatically retried
   - Configurable retry count and delay
   - Progress is preserved between runs

3. Input Validation:
   - Checks file encoding
   - Validates file format
   - Verifies file permissions
   - Handles illegal characters

## Troubleshooting

### Common Issues

1. Chrome Driver Errors:
```bash
# Update Chrome to latest version
google-chrome --version
# Clear Chrome cache
rm -rf ~/.cache/chrome
```

2. Permission Issues:
```bash
# Check directory permissions
chmod 755 ./chunks ./translated_chunks ./translated
```

3. Memory Issues:
- Reduce chunk size in app.py
- Close other applications
- Verify available disk space


## Known Limitations

1. Google Translate Limitations:
   - Subject to Google Translate's daily limits
   - Network dependency
   - Accuracy varies by language pair

2. Technical Limitations:
   - Memory usage scales with chunk size
   - Temporary files require disk space
   - Chrome browser dependency
   - Process pool size may need adjustment based on system resources

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Create a Pull Request