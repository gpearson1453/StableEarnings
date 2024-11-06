import csv
import os

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def check_values_in_csv(file_path):
    large_values = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        
        # Loop through each row and each column to check for values outside the range
        for row_num, row in enumerate(reader, start=1):
            for column, value in row.items():
                try:
                    # Try to convert the value to a float
                    num_value = float(value)
                    # Check if the number is out of the desired range
                    if (num_value > 10000 or num_value < -10000) and column != 'file_number':
                        large_values.append((row_num, column, num_value, row['file_number']))
                except ValueError:
                    # Skip non-numeric values
                    continue

    # Display results
    if large_values:
        print("Values outside the range -10,000 to 10,000 found:")
        for row_num, column, num_value, file_num in large_values:
            print(f"Row {row_num}, Column '{column}': {num_value}, File {file_num}")
    else:
        print("No values outside the range -10,000 to 10,000 found.")

# Example usage
#check_values_in_csv('setup.csv')
check_values_in_csv('traintest.csv')
