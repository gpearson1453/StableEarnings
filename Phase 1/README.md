# Phase 1
Phase 1 of the Stable Earnings project involves downloading all relevant files from Equibase, which are provided in PDF format. These files are reformatted by numbering the PDFs and creating corresponding files to hold their extracted text. The extracted text is then analyzed to retrieve all relevant information, including race results and horse profiles. This data is consolidated into a comprehensive CSV file, which will serve as the foundation for further analysis in the project.

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
`text_segment (str)`: The full text block containing horse-specific information.

Returns:
`List[Dict]`: A list of dictionaries, where each dictionary contains data specific to one horse, such as the program number, horse name, starting position, figures, final position, jockey, trainer, and owner.

Key Features:
PPRLP Data Extraction: Extracts and processes horse performance data (e.g., program number, horse name, starting position, and performance figures) from the PPRLP section of the text.
Trainer and Owner Information: Identifies and extracts trainer and owner details, ensuring the correct assignment to each horse.
Jockey and Weight Data: Uses regex to accurately capture jockey names and associated weights for each horse.
Flexible Handling of Missing Data: Provides default values for missing or incomplete data, ensuring consistent output across all horses.
Final Position Calculation: Automatically assigns a final position to each horse based on the order in which the data appears.

### `Phase 1/getRaces.py`
The `getRaces` function is designed to extract race-specific information from a text block, including the race number, date, location, surface type, distance, weather conditions, and other relevant data. Additionally, it uses the `getHorses` function to retrieve horse-specific data for each race and combines it with the common race data to form a comprehensive dataset.

Parameters:
`text_segment (str)`: The full text block containing race information.

Returns:
`List[Dict]`: A list of dictionaries, where each dictionary contains both common race data and individual horse data. If the race type is not valid, it returns 'Invalid Race Type'.

Key Features:
Handles fractional and split times, final time, and other race-specific metrics.
Automatically generates a unique race ID based on the date, location, and race number.
Validates race type, ensuring only thoroughbred or quarter horse races are processed.
Calls `getHorses` to extract horse-specific data and merges it with the race information.

### `Phase 1/mappings.py`
*Provide a description of the `mappings.py` file here.*

### `Phase 1/processAllToCSV.py`
*Provide a description of the `processAllToCSV.py` file here.*

### `Phase 1/processPPRLP.py`
*Provide a description of the `processPPRLP.py` file here.*

### `Phase 1/processSelectToExcel.py`
*Provide a description of the `processSelectToExcel.py` file here.*

### `Phase 1/reformatRaceFiles.py`
*Provide a description of the `reformatRaceFiles.py` file here.*

---

## Other Files

### `Phase 1/all_race_data.csv`
*Provide a description of the `all_race_data.csv` file here.*

### `Phase 1/mappings.json`
*Provide a description of the `mappings.json` file here.*

### `Phase 1/select_race_data.xlsx`
*Provide a description of the `select_race_data.xlsx` file here.*
