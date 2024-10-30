# Data Processing Pipeline

This repository contains three Python scripts designed to process large datasets by splitting, translating, and merging files. The pipeline is structured in a way that ensures the original data is split into manageable chunks, translated, and then merged back into a final output file. The default download directory is always `/home/Downloads`, and it does not change.

## Files Overview

### 1. `1-splitter.py`
This script is responsible for splitting large CSV files into smaller Excel files. It processes the original file and creates a series of chunks, each containing a fixed number of rows.

- **Input**: A large CSV file (must contain an `id` and `text` column).
- **Output**: Excel files (.xlsx) saved in the `chunks/` folder, with each chunk stored in its own subfolder.

#### Expected Columns:
- `id`: A unique identifier for each row.
- `text`: The text that will be translated.
- Any other metadata or columns in the original CSV file.

#### Example Folder Structure After Splitting:
data-processing-pipeline/ ├── scripts/ │ └── 1-splitter.py ├── chunks/ │ ├── chunk_1/ │ │ └── file_chunk_1.xlsx ....  file_chunk_2.xlsx  │ └── chunk_3/ │ └── file_chunk_3.xlsx ├── README.md └── requirements.txt


### 2. `2-translate.py`
This script translates the text in each chunk using a translation service. The translated text is added as a new column (`translated_text`) to each Excel file.

- **Input**: Excel files from the `chunks/` folder.
- **Output**: Translated Excel files stored in the `downloads/` folder. Each translated file will be saved in the corresponding subfolder for that chunk.

- usage 2-translate.py --target_language kk (example)



### 3. `3-merger.py`
This script merges the translated Excel files back into a single output file. The final merged file is saved in the `final_output/` folder.

- **Input**: Excel files from the `translated_chunks/` folder.
- **Output**: A single Excel file combining all the translated chunks, stored in the `final_output/` folder.

