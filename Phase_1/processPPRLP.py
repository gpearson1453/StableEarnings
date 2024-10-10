def processPPRLP(PPRLP_text):
    """
    Processes the Past Performance Running Line Preview (PPRLP) text block and converts it into a structured
    format by separating headers and values, normalizing the text, and handling special cases like fractions 
    and multi-word entries.

    Steps performed:
    1. Replaces 'Horse Name' with 'Horse' and 'Str 1' with 'Str1' to correct common text issues.
    2. Cleans the text by collapsing multiple spaces into a single space.
    3. Separates headers (e.g., 'Horse', 'Start Pos', etc.) from values.
    4. Handles fractions and multi-word entries to ensure proper formatting.

    Parameters:
    - PPRLP_text (str): The PPRLP text block containing headers and corresponding values.

    Returns:
    - List[List[str]]: A list of lists, where each sublist represents a horse's data extracted from the PPRLP text.
    """

    # Fix common header issues in the text
    PPRLP_text = PPRLP_text.replace('Horse Name', 'Horse').replace('Str 1', 'Str1', 1)

    # Normalize the text by collapsing multiple spaces into a single space
    PPRLP_text = ' '.join(PPRLP_text.split())

    # Find where the headers end and the values begin using 'Fin' as the delimiter
    header_end = PPRLP_text.find('Fin')

    if header_end != -1:
        # Extract headers and values based on the 'Fin' delimiter
        headers = PPRLP_text[:header_end + len('Fin')].strip().split()
        values = PPRLP_text[header_end + len('Fin'):].strip().split()
    else:
        headers, values = [], []

    # Handle fractions by combining values like '21' and '3/4' into '21 3/4'
    i = len(values) - 2
    while i >= 0:
        if '/' in values[i + 1] and len(values[i + 1]) == 3:
            values[i] += ' ' + values[i + 1]
            values.pop(i + 1)
        i -= 1

    # Handle multi-word entries such as horse names
    i = len(values) - 2
    while i >= 0:
        if (not any(char.isdigit() for char in values[i]) and
            not any(char.isdigit() for char in values[i + 1]) and
            values[i] not in ['---', '*'] and values[i + 1] not in ['---', '*']):
            values[i] += ' ' + values[i + 1]
            values.pop(i + 1)
        i -= 1
    
    #this is for debugging when testing specific files using testing_files
    #print(values)
    
    # Create a list of lists where each sublist contains the values for one horse
    val_array = []
    i = 0
    while i < len(values):
        horse_vals = []
        for j in range(len(headers)):
            horse_vals.append(values[i + j])
        val_array.append(horse_vals)
        i += len(headers)

    return val_array
