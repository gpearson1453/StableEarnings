# Phase 1
In Phase 1, all relevant files are downloaded from Equibase, which are provided in PDF format. These files are reformatted by numbering the PDFs and creating corresponding files to hold their extracted text. The extracted text is then analyzed to retrieve all relevant information, including race results and horse profiles. This data is consolidated into a comprehensive CSV file, which will serve as the foundation for further analysis in the project.

## Folders

### `Phase 1/pdf_files`
This folder contains subfolders organized by month and year (e.g., 01-2020). Each subfolder holds the necessary PDF files for the project. These PDFs have been renamed from their original filenames and sequentially numbered to ensure easier reference and usage during processing.

### `Phase 1/testing_files`
This folder is intended to hold copies of text files that present issues during processing. It allows for testing and debugging these files independently from the rest of the dataset. By isolating problematic files, it becomes easier to identify and resolve specific issues without impacting the overall workflow.

### `Phase 1/text_files`
This folder contains all the text files extracted from the PDF files using the `reformatRaceFiles` script.

### `Phase 1/zipped_originals`
This folder holds all the original PDF files, preserved with their original filenames. The files are stored in no-loss compressed folders, each named identically to the corresponding folder in the `pdf_files` directory. This ensures that the original PDFs remain accessible while maintaining a clear organizational structure.

---

## Python Files

### `Phase 1/getHorses.py`
The `getHorses` function is responsible for extracting detailed horse-specific information from a text block, including data from the Past Performance Running Line Preview (PPRLP), trainers, owners, jockeys, and weights. This function processes and combines the extracted information into a structured dataset for each horse in the race.

Parameters:
- `text_segment (str)`: The full text block containing horse-specific information.

Returns:
- `List[Dict]`: A list of dictionaries, where each dictionary contains data specific to one horse, such as the program number, horse name, starting position, figures, final position, jockey, trainer, and owner.

Key Features:
- PPRLP Data Extraction: Extracts and processes horse performance data (e.g., program number, horse name, starting position, and performance figures) from the PPRLP section of the text.
- Trainer and Owner Information: Identifies and extracts trainer and owner details, ensuring the correct assignment to each horse.
- Jockey and Weight Data: Uses regex to accurately capture jockey names and associated weights for each horse.
- Flexible Handling of Missing Data: Provides default values for missing or incomplete data, ensuring consistent output across all horses.
- Final Position Calculation: Automatically assigns a final position to each horse based on the order in which the data appears.

### `Phase 1/getRaces.py`
The `getRaces` function is designed to extract race-specific information from a text block, including the race number, date, location, surface type, distance, weather conditions, and other relevant data. Additionally, it uses the `getHorses` function to retrieve horse-specific data for each race and combines it with the common race data to form a comprehensive dataset.

Parameters:
- `text_segment (str)`: The full text block containing race information.

Returns:
- `List[Dict]`: A list of dictionaries, where each dictionary contains both common race data and individual horse data. If the race type is not valid, it returns 'Invalid Race Type'.

Key Features:
- Handles fractional and split times, final time, and other race-specific metrics.
- Automatically generates a unique race ID based on the date, location, and race number.
- Validates race type, ensuring only thoroughbred or quarter horse races are processed.
- Calls `getHorses` to extract horse-specific data and merges it with the race information.

### `Phase 1/mappings.py`
The `mappings.py` module manages various mappings used throughout the program, including horse name corrections, distance conversions, surface mappings, and race type identification. This module also handles loading and saving custom mappings from a JSON file.

Key Features:

- Custom Mappings Handling: Loads and saves horse name corrections (stupid_horse_names) and other mappings from a JSON file, ensuring customizations persist across runs.
- Distance Conversion: Provides a dictionary for converting race distances (e.g., furlongs and yards) into miles, which is used in race data extraction.
- Surface Type Mapping: Maps surface abbreviations (e.g., 'D', 'T', 'A') to full names like 'Dirt', 'Turf', and 'AWT'.
- Race Type Identification: Uses regular expressions to classify races based on type (e.g., 'Thoroughbred', 'Quarter Horse', etc.), ensuring only valid race types are processed.

### `Phase 1/processAllToCSV.py`
The `processAllToCSV.py` module processes text files containing race data **from all folders**, extracts relevant information, and outputs the data into a **CSV** file. It uses multi-threading to process multiple files concurrently, ensuring efficient performance even with large datasets.

Key Features:

- Text Splitting: Splits the full text of each file into segments based on a predefined marker (All Rights Reserved.) and processes each segment individually.
- Horse Name Fixes: Automatically replaces problematic horse names based on a predefined dictionary (`stupid_horse_names`), and tracks the changes in a fixed dictionary.
- Concurrent Processing: Uses multi-threading to process multiple files at once, speeding up the extraction process.
- Data Extraction: Calls the `getRaces` function to extract detailed race data from each segment, skipping segments that refer to canceled events or invalid race types.
- CSV Output: Writes the extracted data to a CSV file, ensuring that it is sorted by file number and race number, and includes all relevant columns.

### `Phase 1/processPPRLP.py`
The `processPPRLP.py` module processes the Past Performance Running Line Preview (PPRLP) text block and structures it into a usable format by extracting headers and values, normalizing the text, and handling special cases like fractions and multi-word entries.

Parameters:
- `PPRLP_text (str)`: The PPRLP text block that contains both headers and values to be processed. The text is expected to follow a structured format where headers (like 'Horse', 'Start Pos', etc.) are followed by corresponding values.

Returns:
- `List[List[str]]`: A list of lists, where each sublist represents the data associated with one horse. Each sublist contains values for the horse that correspond to the extracted headers.

Key Features:

- Text Normalization: Cleans the PPRLP text by collapsing multiple spaces into a single space and fixing common text issues.
- Header and Value Extraction: Separates headers from corresponding values based on the location of 'Fin' in the text.
- Special Case Handling: Deals with fractions and multi-word entries to ensure proper formatting, such as combining '21' and '3/4' into '21 3/4' or multi-word horse names.
- Structured Output: Returns a list of lists where each sublist contains all the data for a single horse, extracted and formatted correctly.

### `Phase 1/processSelectToExcel.py`
The `processSelectToExcel.py` module processes text files containing race data **from a specific folder**, as opposed to all folders, extracts relevant information, and outputs the data into an **Excel file**. It uses multi-threading to handle multiple files concurrently, making it efficient for processing large datasets.

Key Features:
- Text Splitting: Splits the full text of each file into segments based on a predefined marker (All Rights Reserved.) and processes each segment individually.
- Horse Name Fixes: Automatically replaces problematic horse names based on a predefined dictionary (`stupid_horse_names`) and tracks the changes in a fixed dictionary.
- Concurrent Processing: Uses multi-threading to process multiple files concurrently, improving performance.
- Data Extraction: Calls the `getRaces` function to extract race-specific data from each segment, skipping segments that refer to canceled events or invalid race types.
Excel Output: Writes the extracted data to an Excel file, ensuring that it is sorted by file number and race number. The first row of the Excel file is frozen for improved readability.

### `Phase 1/reformatRaceFiles.py`
The `reformatRaceFiles.py` module renames PDF files within subfolders of `pdf_files` in a sequential manner and extracts the text from each renamed PDF, saving the text in corresponding text files. This helps standardize file names and store text data for further analysis.

Key Features:
- Sequential File Renaming: Renames PDF files in each subfolder by numbering them sequentially. The new names follow the pattern: number_subfolder.pdf. If a file with the new name already exists, a version suffix (e.g., _v1) is added.
- Text Extraction from PDF: Extracts text from each renamed PDF using the PyMuPDF library (fitz), and stores the text in a corresponding .txt file in a `text_files` folder.
- Folder Structure Preservation: The structure of the original subfolders is maintained in the `text_files` folder, with each subfolder getting its own corresponding folder for storing extracted text files.

---

## Other Files

### `Phase 1/all_race_data.csv`
This CSV file is generated by `processAllToCSV.py` and contains all the data extracted from the PDF files. It aggregates all relevant race and horse information into one comprehensive dataset.

### `Phase 1/mappings.json`
This JSON file stores mappings of problematic horse names, which may cause issues in the code, along with the corresponding corrected versions. These mappings are used by `mappings.py` to replace problematic names in the extracted text files.

### `Phase 1/select_race_data.xlsx`
This Excel file is created by `processSelectToExcel.py` to allow for easy examination of data from a specific folder, enabling a more focused review compared to the comprehensive CSV file.
