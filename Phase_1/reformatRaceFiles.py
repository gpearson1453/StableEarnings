import os  # For file and directory operations
import fitz  # PyMuPDF for reading PDF files

# Automatically set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Directories
pdfs_dir = "pdf_files"
text_files_dir = "text_files"


def reformat_files_sequentially(pdfs_folder, text_folder_base):
    """
    Renames PDF files in a sequential manner and extracts text from each PDF, saving it in a corresponding text file.

    The function performs the following steps:
    1. Iterates over each subfolder in the PDFs folder.
    2. Renames the files inside each subfolder sequentially and stores the renamed files in the same subfolder.
    3. Extracts text from the renamed PDFs and stores it in text files in a corresponding subfolder in the text folder base.

    Parameters:
    - pdfs_folder (str): Path to the folder containing the subfolders with PDF files.
    - text_folder_base (str): Path to the base folder where extracted text files will be stored.
    """
    current_number = 1  # Start numbering from 1

    # Iterate over each subfolder in the PDFs directory
    for folder in os.listdir(pdfs_folder):
        folder_path = os.path.join(pdfs_folder, folder)
        if not os.path.isdir(folder_path):
            print(f"{folder_path} is not a directory. Skipping.")
            continue

        # Create a corresponding folder in text_files if it doesn't already exist
        text_folder = os.path.join(text_folder_base, folder)
        if not os.path.exists(text_folder):
            os.makedirs(text_folder)

        # Get all files in the folder and their creation times
        files_with_times = [
            (file, os.path.getctime(os.path.join(folder_path, file)))
            for file in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, file))
        ]

        # Sort files by creation time
        files_with_times.sort(key=lambda x: x[1])

        # Process each file in the folder
        for file, _ in files_with_times:
            old_file_path = os.path.join(folder_path, file)
            new_file_name = f"{current_number}_{folder}.pdf"
            new_file_path = os.path.join(folder_path, new_file_name)

            # Avoid conflicts by appending a version suffix if the new file name already exists
            counter = 1
            while os.path.exists(new_file_path):
                new_file_name = f"{current_number}_{folder}_v{counter}.pdf"
                new_file_path = os.path.join(folder_path, new_file_name)
                counter += 1

            # Rename the file
            os.rename(old_file_path, new_file_path)
            print(f"Renamed: {old_file_path} to {new_file_path}")

            # Extract text from the PDF and save it in a corresponding text file
            text_file_name = f"{current_number}_{folder}.txt"
            text_file_path = os.path.join(text_folder, text_file_name)

            try:
                with fitz.open(new_file_path) as pdf_file:
                    full_text = ""
                    for page_num in range(len(pdf_file)):
                        page = pdf_file.load_page(page_num)
                        full_text += page.get_text("text")

                # Save the extracted text to the text file
                with open(text_file_path, "w", encoding="utf-8") as text_file:
                    text_file.write(full_text)

                print(f"Extracted text to: {text_file_path}")

            except Exception as exc:
                print(f"Error extracting text from {new_file_path}: {exc}")

            current_number += 1  # Increment the number for the next file


# Call the function with the 'pdf_files' and 'text_files' directories
reformat_files_sequentially(pdfs_dir, text_files_dir)
