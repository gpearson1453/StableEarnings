"""
reformatRaceFiles.py renames race PDF files sequentially and extracts their text content.

This script processes folders of PDF files, renaming them in a sequential order based on their creation time and extracting
their text content into corresponding text files. The extracted text files are saved in a folder named after the original
folder where the PDF files were found.

Steps:
    - Iterates through folders within a specified PDF directory.
    - Renames PDF files sequentially, ensuring unique names even in case of conflicts.
    - Extracts text content from each PDF and saves it as a text file in the corresponding output folder.

Functions:
    - reformat_files_sequentially: Handles renaming and text extraction for all files in a given PDF folder.

Usage:
    Execute the script directly to process files in the 'pdf_files' folder and output the results to 'text_files'.
    Ensure the input and output folder paths are configured as needed before execution.
"""
import os
import fitz

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Define the input and output directories
pdfs_dir = "pdf_files"
text_files_dir = "text_files"


def reformat_files_sequentially(pdfs_folder, text_folder_base):
    """
    Process PDF files in a folder by renaming them sequentially and extracting their text content.

    Args:
        pdfs_folder (str): Path to the folder containing PDF files to process.
        text_folder_base (str): Base path for the output folders containing extracted text files.
    """
    current_number = 1

    for folder in os.listdir(pdfs_folder):
        folder_path = os.path.join(pdfs_folder, folder)
        if not os.path.isdir(folder_path):
            print(f"{folder_path} is not a directory. Skipping.")
            continue

        # Create a corresponding text folder if it does not exist
        text_folder = os.path.join(text_folder_base, folder)
        if not os.path.exists(text_folder):
            os.makedirs(text_folder)

        # Get files sorted by creation time
        files_with_times = [
            (file, os.path.getctime(os.path.join(folder_path, file)))
            for file in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, file))
        ]

        files_with_times.sort(key=lambda x: x[1])

        for file, _ in files_with_times:
            old_file_path = os.path.join(folder_path, file)
            new_file_name = f"{current_number}_{folder}.pdf"
            new_file_path = os.path.join(folder_path, new_file_name)

            # Handle name conflicts by appending a version number
            counter = 1
            while os.path.exists(new_file_path):
                new_file_name = f"{current_number}_{folder}_v{counter}.pdf"
                new_file_path = os.path.join(folder_path, new_file_name)
                counter += 1

            # Rename the PDF file
            os.rename(old_file_path, new_file_path)
            print(f"Renamed: {old_file_path} to {new_file_path}")

            # Extract text content from the renamed PDF file
            text_file_name = f"{current_number}_{folder}.txt"
            text_file_path = os.path.join(text_folder, text_file_name)

            try:
                with fitz.open(new_file_path) as pdf_file:
                    full_text = ""
                    for page_num in range(len(pdf_file)):
                        page = pdf_file.load_page(page_num)
                        full_text += page.get_text("text")

                # Save the extracted text to a file
                with open(text_file_path, "w", encoding="utf-8") as text_file:
                    text_file.write(full_text)

                print(f"Extracted text to: {text_file_path}")

            except Exception as exc:
                print(f"Error extracting text from {new_file_path}: {exc}")

            # Increment the file counter
            current_number += 1


if __name__ == "__main__":
    """
    Main script execution. Processes PDF files in the 'pdf_files' folder, renames them sequentially, and extracts their text
    content into corresponding text files in the 'text_files' folder.
    """
    reformat_files_sequentially(pdfs_dir, text_files_dir)
