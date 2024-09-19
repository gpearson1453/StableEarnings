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
*Provide a description of the `getHorses.py` file here.*

### `Phase 1/getRaces.py`
*Provide a description of the `getRaces.py` file here.*

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
