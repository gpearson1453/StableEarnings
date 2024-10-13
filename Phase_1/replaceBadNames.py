import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def replace_string_in_file(file_path, target_string, replacement_string):
    try:
        # Open the file and read the contents
        with open(file_path, 'r', encoding='utf-8') as f:
            file_contents = f.read()

        # If the target string is found, proceed with replacement
        if target_string in file_contents[file_contents.find('Past Performance Running Line Preview'):]:
            updated_contents = file_contents.replace(target_string, replacement_string)

            # Only write back to the file if changes were made
            if updated_contents != file_contents:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(updated_contents)
                
                # Print the file and target string that was replaced
                print(f"Replaced '{target_string}' in {file_path}")
    except Exception as e:
        # Include the target string in the error message
        print(f"Error processing {file_path} with target string '{target_string}': {e}")

def process_folder(folder_name, target_string, replacement_string):
    folder_path = os.path.join(os.getcwd(), folder_name)
    tasks = []
    
    with ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.txt'):  # Customize this filter if needed
                    file_path = os.path.join(root, file)
                    # Submit the task to the executor pool
                    tasks.append(executor.submit(replace_string_in_file, file_path, target_string, replacement_string))
                    
        # Process results as they complete
        for future in as_completed(tasks):
            future.result()  # This will execute the task but not print the result for successful replacements

# Example usage
folder_to_search = 'text_files'  # Replace with the name of your folder
target_string = 'old_string'  # Replace with the string you want to replace
replacement_string = 'new_string'  # Replace with the replacement string

#this should contain tuples
both_names = [('Jc Howthewest Waswon1', 'Jc Howthewest Waswon 1'), ('Jc Howthewest Waswon2', 'Jc Howthewest Waswon 2'), ('Jc Howthewest Waswon3', 'Jc Howthewest Waswon 3'), ('Jc Howthewest Waswon4', 'Jc Howthewest Waswon 4'), ('Jc Howthewest Waswon5', 'Jc Howthewest Waswon 5'),
                ('Jc Howthewest Waswon6', 'Jc Howthewest Waswon 6'), ('Jc Howthewest Waswon7', 'Jc Howthewest Waswon 7'), ('Jc Howthewest Waswon8', 'Jc Howthewest Waswon 8'), ('Jc Howthewest Waswon9', 'Jc Howthewest Waswon 9'), ('Jc Howthewest Waswon10', 'Jc Howthewest Waswon 10')]

#this should not contain tuples
names = []

# Run all names in names list, only use if just replacing spaces with hyphens
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_folder, folder_to_search, name, name.replace(' ', '-')) for name in names]
    
    # Wait for all tasks to complete
    for future in as_completed(futures):
        future.result()

# this version is used for more specific cases that nee specific replacements
# each entity in names is a tuple of a target and replacement string        
'''with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_folder, folder_to_search, name[0], name[1]) for name in both_names]
    
    # Wait for all tasks to complete
    for future in as_completed(futures):
        future.result()'''
