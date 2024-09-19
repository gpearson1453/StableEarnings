import fitz  # Import PyMuPDF for handling PDF files
import os  # Import os for directory operations
from processFiles import splitText  # Import splitText from processFiles.py

# Example function to process PDFs as in the original code (this can be independent now)
def process_pdfs():
    # List of PDF file paths to process
    filePaths = ['162_January2020.pdf', '169_January2020.pdf']
    
    # Folder path to save the output text files
    to_folder_path = 'inspectFiles'
    
    # Folder path to get the pdfs from
    from_folder_path = 'January2020'
    
    # Bool to indicate if you want to clear out TextFiles
    emptyFolder = True
    
    # Check if the output folder exists, if not, create it
    if not os.path.exists(to_folder_path):
        os.makedirs(to_folder_path)
    
    # Clear out the output folder if emptyFolder is set to True
    if emptyFolder:
        for file in os.listdir(to_folder_path):
            file_path = os.path.join(to_folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    
    # Check if there are any files to process
    if not len(filePaths) == 0:
        # Loop through each PDF file in the list
        for filePath in filePaths:
            # Construct the full file path
            full_file_path = os.path.join(from_folder_path, filePath)
            
            # Open the PDF file
            pdf_document = fitz.open(full_file_path)
            num_pages = pdf_document.page_count  # Get the number of pages in the PDF
            full_text = ''  # Initialize an empty string to store the full text
    
            # Loop through each page of the PDF
            for page_num in range(num_pages):
                page = pdf_document.load_page(page_num)  # Load the page
                text = page.get_text()  # Extract text from the page
                full_text += text  # Append the text to full_text
    
            # Close the PDF document
            pdf_document.close()
    
            # Call splitText method to split the full text
            split_texts = splitText(full_text)
    
            # Create a new .txt file with the same name as the PDF
            base_name = os.path.basename(filePath).replace('.pdf', '.txt')
            txt_file_path = os.path.join(to_folder_path, base_name)
    
            # Write the split parts to the new .txt file
            with open(txt_file_path, 'w') as txt_file:
                for part in split_texts:
                    txt_file.write(part.strip() + (10 * '\n'))  # Write each part followed by a newline

if __name__ == "__main__":
    process_pdfs()
