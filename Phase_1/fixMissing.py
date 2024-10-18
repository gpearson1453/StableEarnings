import os
import fitz  # PyMuPDF for PDF reading
import csv

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Directories
pdfs_dir = 'missing_pdfs'
text_files_dir = 'text_files'
csv_file = 'all_race_data.csv'

def get_last_file_number(csv_file):
    """Reads the last file number from all_race_data.csv."""
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
        last_row = rows[-1]
        return int(last_row[0])  # Assumes the file number is in the first column

def find_target_folder(pdf_name):
    """Finds the corresponding folder in text_files based on the date in the PDF name."""
    parts = pdf_name.split('_')[0].split(' ')
    month, day, year = parts[0], parts[1].replace(',', ''), parts[2]
    
    # Convert the month to number
    months = {"January": "01", "February": "02", "March": "03", "April": "04", "May": "05", 
              "June": "06", "July": "07", "August": "08", "September": "09", "October": "10", 
              "November": "11", "December": "12"}
    month_number = months[month]
    
    # Find matching folders in text_files
    year_month = f"{year}-{month_number}"
    target_folders = [f for f in os.listdir(text_files_dir) if f.startswith(year_month)]
    
    # If both A and B folders exist, choose the B one
    if len(target_folders) == 2:
        folder = os.path.join(text_files_dir, [f for f in target_folders if f.endswith('-B')][0])
        return folder
    
    # Otherwise, choose the one that exists
    folder = os.path.join(text_files_dir, target_folders[0])
    return folder

def get_missing_or_max_number(folder, starting_number):
    """Finds a missing file number or the next available file number in the folder."""
    existing_files = [f for f in os.listdir(folder) if f.startswith('File')]
    existing_numbers = sorted([int(f.split('_')[0]) for f in existing_files])
    
    # Check for missing numbers
    for i in range(starting_number, max(existing_numbers, default=starting_number) + 1):
        if i not in existing_numbers:
            print(f"Using missing number: {i}")
            return i
    
    # If no missing numbers, return the next number
    next_number = max(existing_numbers, default=starting_number) + 1
    print(f"Using next incremented number: {next_number}")
    return next_number

def extract_text_from_pdf(pdf_path):
    """Extracts text from a PDF file."""
    full_text = ""
    try:
        with fitz.open(pdf_path) as pdf_file:
            for page_num in range(len(pdf_file)):
                page = pdf_file.load_page(page_num)
                full_text += page.get_text("text")
    except Exception as e:
        print(f"Error extracting text from PDF: {pdf_path}, Error: {e}")
    return full_text

def save_text_to_file(folder, file_number, folder_name, text):
    """Saves extracted text to a file in the specified folder."""
    text_file_name = f"{file_number}_{folder_name}.txt"
    text_file_path = os.path.join(folder, text_file_name)
    try:
        with open(text_file_path, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f"New file created: {text_file_name} in folder: {folder}")
    except Exception as e:
        print(f"Error saving text to file: {text_file_path}, Error: {e}")

def process_missing_pdfs(pdfs_folder, text_folder_base, csv_file):
    """Processes missing PDFs and saves the extracted text to the correct folder in text_files."""
    # Get the starting file number from the CSV file
    current_number = get_last_file_number(csv_file) + 1

    # Iterate over each PDF in missing_pdfs
    for pdf_file in os.listdir(pdfs_folder):
        if not pdf_file.endswith('.pdf'):
            continue
        
        pdf_path = os.path.join(pdfs_folder, pdf_file)
        
        # Find the target folder in text_files
        folder = find_target_folder(pdf_file)
        
        # Get the missing or next file number
        file_number = get_missing_or_max_number(folder, current_number)
        
        # Extract text from the PDF
        text = extract_text_from_pdf(pdf_path)
        
        # Save the text to the correct folder
        save_text_to_file(folder, file_number, os.path.basename(folder), text)
        
        # Update the current number for the next file
        current_number = file_number + 1

# Call the function to process missing PDFs
process_missing_pdfs(pdfs_dir, text_files_dir, csv_file)
