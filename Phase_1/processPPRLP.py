"""
processPPRLP.py processes the Past Performance Running Line Preview (PPRLP) text block.

This script defines a function to extract structured horse performance data from the PPRLP block.
It splits the input text into headers and values, processes inconsistencies, and returns the data
in a structured format.

Steps:
    - Replace problematic header names to ensure consistent formatting.
    - Remove excessive whitespace and merge split lines.
    - Split the text into headers and values based on the "Fin" delimiter.
    - Address errors caused by unconventional formatting of horse names or numerical values.
    - Return the processed values as a list of lists, where each list represents a horse's data.

Functions:
    - processPPRLP: Main function to process PPRLP text and return structured data.

Usage:
    Import and call the processPPRLP function from other scripts that need to process PPRLP text blocks.
"""


def processPPRLP(PPRLP_text):
    """
    Process the Past Performance Running Line Preview (PPRLP) text block.

    Args:
        PPRLP_text (str): The input PPRLP text block.

    Returns:
        list: A list of lists where each sublist contains structured data for a single horse.
    """
    # Replace inconsistent headers to ensure proper formatting
    PPRLP_text = PPRLP_text.replace("Horse Name", "Horse").replace("Str 1", "Str1", 1)

    # Normalize whitespace
    PPRLP_text = " ".join(PPRLP_text.split())

    # Split the text into headers and values at the "Fin" delimiter
    header_end = PPRLP_text.find("Fin")

    if header_end != -1:
        headers = PPRLP_text[: header_end + len("Fin")].strip().split()
        values = PPRLP_text[header_end + len("Fin"):].strip().split()
    else:
        headers, values = [], []

    # Merge split numerical values, e.g., fractions
    i = len(values) - 2
    while i >= 0:
        if "/" in values[i + 1] and len(values[i + 1]) == 3:
            values[i] += " " + values[i + 1]
            values.pop(i + 1)
        i -= 1

    # Merge split names or entries without digits (likely part of horse names)
    i = len(values) - 2
    while i >= 0:
        if (
            not any(char.isdigit() for char in values[i])
            and not any(char.isdigit() for char in values[i + 1])
            and values[i] not in ["---", "*"]
            and values[i + 1] not in ["---", "*"]
        ):
            values[i] += " " + values[i + 1]
            values.pop(i + 1)
        i -= 1

    # Most errors that occur with this file come from this next block of code and are caused by weird horse names, like
    # really long names or names with numbers. The following print statement is used in debugging to be able to see the
    # horse names (and other data) that are being processed when an error is thrown.
    # print(values)

    # Convert headers and values into a structured list of lists
    val_array = []
    i = 0
    while i < len(values):
        horse_vals = []
        for j in range(len(headers)):
            horse_vals.append(values[i + j])
        val_array.append(horse_vals)
        i += len(headers)

    return val_array
