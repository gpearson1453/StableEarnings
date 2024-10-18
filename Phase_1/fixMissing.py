import os
import csv
from datetime import datetime
import fitz  # For text extraction from PDFs
import shutil  # For moving files

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def get_next_max(csv_file):
    # Open the CSV file and read its content
    with open(csv_file, mode='r', newline='') as file:
        reader = csv.reader(file)
        rows = list(reader)  # Convert reader to a list of rows

        # Check if there are any rows
        if rows:
            # Get the first column of the last row and increment by 1
            try:
                last_row = rows[-1]
                next_max = int(last_row[0]) + 1
                return next_max
            except ValueError:
                print("Error: The value in the first column is not an integer.")
        else:
            print("Error: The CSV file is empty.")
            return None

def find_subfolder_for_pdf(pdf_filename, text_files_dir):
    base_filename = os.path.splitext(pdf_filename)[0]
    date_part, track_name = base_filename.split('_')

    try:
        pdf_date = datetime.strptime(date_part, '%B %d, %Y')
    except ValueError:
        print(f"Error: Could not parse date from filename '{pdf_filename}'")
        return None

    year = pdf_date.year
    month = pdf_date.strftime('%m')
    base_folder_name = f"{year}-{month}"

    folder_b = os.path.join(text_files_dir, f"{base_folder_name}-B")
    folder_a = os.path.join(text_files_dir, f"{base_folder_name}-A")
    folder_base = os.path.join(text_files_dir, base_folder_name)

    if os.path.exists(folder_b):
        return folder_b
    elif os.path.exists(folder_a):
        return folder_a
    elif os.path.exists(folder_base):
        return folder_base
    else:
        print(f"No matching folder found for {pdf_filename}")
        return None

def find_next_available_number(subfolder):
    files = [f for f in os.listdir(subfolder) if f.endswith('.txt')]
    used_numbers = [int(f.split('_')[0]) for f in files]
    
    if not used_numbers:
        return None
    
    for num in range(min(used_numbers), max(used_numbers)):
        if num not in used_numbers:
            return num
    
    return None

def extract_and_save_text(pdf_path, text_file_path):
    try:
        with fitz.open(pdf_path) as pdf_file:
            full_text = ""
            for page_num in range(len(pdf_file)):
                page = pdf_file.load_page(page_num)
                full_text += page.get_text("text")

        with open(text_file_path, 'w', encoding='utf-8') as text_file:
            text_file.write(full_text)
        print(f"Extracted text to: {text_file_path}")

    except Exception as exc:
        print(f"Error extracting text from {pdf_path}: {exc}")

def rename_and_move_pdf(pdf_file_path, new_pdf_file_path):
    try:
        shutil.move(pdf_file_path, new_pdf_file_path)
        print(f"Renamed and moved PDF to: {new_pdf_file_path}")
    except Exception as e:
        print(f"Error moving the PDF file: {e}")

# Usage
csv_file = 'all_race_data.csv'
next_max = get_next_max(csv_file)

missing_pdfs_dir = 'missing_pdfs'
text_files_dir = 'text_files'
pdf_files_dir = 'pdf_files'  # New directory for storing PDFs

# Loop through all PDF files in missing_pdfs folder
for pdf_file in os.listdir(missing_pdfs_dir):
    if pdf_file.endswith('.pdf'):
        subfolder = find_subfolder_for_pdf(pdf_file, text_files_dir)
        if subfolder:
            print(f"{pdf_file} -> {subfolder}")
            pdf_path = os.path.join(missing_pdfs_dir, pdf_file)

            # Find the smallest unused number or use next_max
            next_number = find_next_available_number(subfolder)
            if next_number is None:
                next_number = next_max
                next_max += 1

            # Extract the year-month part from the subfolder name
            folder_name = os.path.basename(subfolder)
            base_folder_name = folder_name.split('-')[0] + '-' + folder_name.split('-')[1]

            # Name the new text file based on the found number and subfolder name
            text_file_name = f"{next_number}_{base_folder_name}.txt"
            text_file_path = os.path.join(subfolder, text_file_name)

            # Extract text and save it
            extract_and_save_text(pdf_path, text_file_path)

            # Rename the PDF using the same convention
            new_pdf_file_name = f"{next_number}_{base_folder_name}.pdf"
            pdf_subfolder = os.path.join(pdf_files_dir, folder_name)  # Use the same subfolder name in pdf_files
            if not os.path.exists(pdf_subfolder):
                os.makedirs(pdf_subfolder)

            new_pdf_file_path = os.path.join(pdf_subfolder, new_pdf_file_name)

            # Rename and move the PDF to the corresponding subfolder in pdf_files
            rename_and_move_pdf(pdf_path, new_pdf_file_path)
