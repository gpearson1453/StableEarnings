def processPPRLP(PPRLP_text):
    """
    Process the Past Performance Running Line Preview (PPRLP) text block and convert it into a structured
    format by separating headers and values, normalizing the text, and handling special cases like fractions
    and multi-word entries.

    This function performs the following steps:
    1. Replaces 'Horse Name' with 'Horse' and 'Str 1' with 'Str1' to fix common text issues.
    2. Cleans the text by replacing multiple spaces with a single space.
    3. Separates headers (e.g., columns like 'Horse', 'Start Pos', etc.) from values (e.g., the data under each header).
    4. Handles special cases such as fractions and multi-word horse names that need to be concatenated properly.

    Parameters:
    - PPRLP_text: A string containing the PPRLP text block with headers and corresponding values.

    Returns:
    - A list of lists (val_array), where each sublist corresponds to a horse's data. Each sublist contains the values
      associated with the headers.
    """

    # Replace 'Horse Name' with 'Horse' to fix naming issues in the text
    PPRLP_text = PPRLP_text.replace('Horse Name', 'Horse')
    
    # Replace 'Str 1' with 'Str1' once to fix a common header issue where 'Str' and '1' are separated
    PPRLP_text = PPRLP_text.replace('Str 1', 'Str1', 1)

    # Normalize the text by replacing multiple spaces with a single space
    PPRLP_text = ' '.join(PPRLP_text.split())

    # Identify where the headers end and the values begin, using 'Fin' as the delimiter
    header_end = PPRLP_text.find('Fin')

    if header_end != -1:
        # Extract headers (everything before 'Fin') and split into a list
        headers = PPRLP_text[:header_end + len('Fin')].strip().split()
        # Extract values (everything after 'Fin') and split into a list
        values = PPRLP_text[header_end + len('Fin'):].strip().split()
    else:
        # If headers or values are not found, return empty lists
        headers = []
        values = []
        
    # Handle fractions in the values (e.g., "21 3/4" should be combined as a single value)
    # Fractions are typically found with two parts separated by a '/'.
    # Check if the fraction's length is 3 (e.g., '3/4') to ensure proper concatenation.
    i = len(values) - 2
    while i >= 0:
        if '/' in values[i+1] and len(values[i+1]) == 3:
            values[i] += ' ' + values[i+1]  # Combine the fraction with the preceding value
            values.pop(i+1)  # Remove the second part of the fraction from the list
        i -= 1

    # Handle multi-word entries in the values (e.g., multi-word horse names like "Fast Runner").
    # If two consecutive values have no digits and are not special characters ('---', '*'),
    # they are assumed to be part of the same name or phrase and are concatenated.
    i = len(values) - 2
    while i >= 0:
        if (
            not any(char.isdigit() for char in values[i]) and 
            not any(char.isdigit() for char in values[i+1]) and 
            not values[i] == '---' and 
            not values[i+1] == '---' and
            not values[i] == '*' and 
            not values[i+1] == '*'):
            values[i] += ' ' + values[i+1]  # Concatenate multi-word entries
            values.pop(i+1)  # Remove the second part from the list
        i -= 1

    # Create a list of lists where each sublist contains the values for a single horse
    val_array = []
    i = 0
    while i < len(values):
        horse_vals = []
        # For each header, extract the corresponding value for this horse
        for j in range(len(headers)):
            horse_vals.append(values[i+j])
        val_array.append(horse_vals)  # Append the horse's values to the final array
        i += len(headers)  # Move to the next set of values
    
    return(val_array)
