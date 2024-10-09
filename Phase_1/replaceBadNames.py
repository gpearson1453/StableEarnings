import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def replace_string_in_file(file_path, target_string, replacement_string):
    try:
        # Open the file, read the contents, and replace the target string
        with open(file_path, 'r', encoding='utf-8') as f:
            file_contents = f.read()

        # Replace the target string with the replacement string
        updated_contents = file_contents.replace(target_string, replacement_string)

        # Write the updated contents back to the file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(updated_contents)
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

names = ['Air Born Jazz 2', 'Yadi 1', 'Hes Just 44', 'Flying Eagle 07', 'Tellmshesa 10', 'La Pistola 03', 'Country Boy 123',
         'Aj Chick in 15', 'Super Dominyun 911', 'Lynnder 16', 'Picacho 369', 'I R a Lacey J 2', 'Gulfstream 650',
         'Fireball Xl5', 'Train 214', 'Jess B 613', 'Dynasty Champ 123', 'Mag 1', 'Hangar 24', 'Wicked 6']

# Single run
# process_folder(folder_to_search, target_string, replacement_string)

# Run all names in names list, only use if just replacing spaces with hyphens
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_folder, folder_to_search, name, name.replace(' ', '-')) for name in names]
    
    # Wait for all tasks to complete
    for future in as_completed(futures):
        future.result()
