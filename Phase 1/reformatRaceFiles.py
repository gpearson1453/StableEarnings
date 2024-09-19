import os  # For file and directory operations
import fitz  # PyMuPDF for reading PDF files

# Directories
originals_dir = 'originals'
text_files_dir = 'text_files'

# Function to rename files and extract text
def reformat_files_sequentially(originals_folder, text_folder_base):
    current_number = 1  # Start the numbering from 1

    # Loop through each subfolder in the 'originals' directory
    for folder in os.listdir(originals_folder):
        folder_path = os.path.join(originals_folder, folder)
        if not os.path.isdir(folder_path):
            print(f"{folder_path} is not a directory. Skipping.")
            continue

        # Create a corresponding folder in 'text_files'
        text_folder = os.path.join(text_folder_base, folder)
        if not os.path.exists(text_folder):
            os.makedirs(text_folder)

        # Get all files in the folder along with their creation times
        files_with_times = [(file, os.path.getctime(os.path.join(folder_path, file))) for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]

        # Sort the files by creation time
        files_with_times.sort(key=lambda x: x[1])

        # Process each file in the folder
        for file, _ in files_with_times:
            old_file_path = os.path.join(folder_path, file)
            new_file_name = f"{current_number}_{folder}.pdf"
            new_file_path = os.path.join(folder_path, new_file_name)

            # Check if the new file name already exists, if so, append a suffix to avoid conflict
            counter = 1
            while os.path.exists(new_file_path):
                new_file_name = f"{current_number}_{folder}_v{counter}.pdf"
                new_file_path = os.path.join(folder_path, new_file_name)
                counter += 1

            # Rename the file
            os.rename(old_file_path, new_file_path)
            print(f"Renamed: {old_file_path} to {new_file_path}")

            # Extract text from the PDF and save it to a new text file in the "text_files" folder
            text_file_name = f"{current_number}_{folder}.txt"
            text_file_path = os.path.join(text_folder, text_file_name)

            try:
                with fitz.open(new_file_path) as pdf_file:
                    full_text = ""
                    for page_num in range(len(pdf_file)):
                        page = pdf_file.load_page(page_num)
                        full_text += page.get_text("text")

                # Save the extracted text into the text file
                with open(text_file_path, 'w', encoding='utf-8') as text_file:
                    text_file.write(full_text)

                print(f"Extracted text to: {text_file_path}")

            except Exception as exc:
                print(f"Error extracting text from {new_file_path}: {exc}")

            current_number += 1  # Increment the number for the next file

# Call the function with 'originals' and 'text_files' folders
reformat_files_sequentially(originals_dir, text_files_dir)
