from processPPRLP import processPPRLP
import re

def getHorses(text_segment):
    """
    Extract horse-specific data from a text segment, including PPRLP data, trainers, owners, jockeys, 
    and weights. This function processes the Past Performance Running Line Preview (PPRLP) block, 
    combines it with trainer and owner information, and then organizes the extracted data into a 
    list of dictionaries for each horse.

    Parameters:
    - text_segment: A string containing the full text block from which horse-related data will be extracted.

    Returns:
    - A list of dictionaries, where each dictionary contains horse-specific data combined with common race data.
    """

    # Define the phrases that indicate the start and end of key text blocks
    start_phrase = 'Past Performance Running Line Preview'
    end_phrase = 'Trainers:'
    owners_end_phrase = 'Footnotes'
    
    # Find the start and end indices for the PPRLP section
    start_idx = text_segment.find(start_phrase)
    end_idx = text_segment.find(end_phrase)
    
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        # Extract and clean the PPRLP text block (remove newlines)
        PPRLP_text = text_segment[start_idx + len(start_phrase):end_idx].strip()
        PPRLP_text = PPRLP_text.replace('\n', ' ')

        # Call processPPRLP to process the PPRLP text block into a list of lists
        pprlp_data = processPPRLP(PPRLP_text)
    else:
        # If PPRLP section not found, mark it as "NOT FOUND"
        PPRLP_text = "NOT FOUND"
        pprlp_data = []

    # Extract the trainer section (text between 'Trainers:' and 'Owners:')
    owners_start_idx = text_segment.find('Owners:')
    if owners_start_idx != -1:
        # Trainers section ends at the start of the 'Owners' section
        trainers_section = text_segment[end_idx + len('Trainers:'):owners_start_idx].strip()
    else:
        # If no 'Owners:' section, the trainers section goes to the end of the segment
        trainers_section = text_segment[end_idx + len('Trainers:'):].strip()

    # Split trainers by ';' and extract the trainer names (after the '-')
    trainers_list = trainers_section.split(';')
    trainers_list = [trainer.split('-')[1].strip() for trainer in trainers_list if '-' in trainer]

    # Extract the owners section (starts after 'Owners:' and ends before 'Footnotes')
    footnotes_idx = text_segment.find(owners_end_phrase)
    if owners_start_idx != -1 and footnotes_idx != -1:
        owners_section = text_segment[owners_start_idx + len('Owners: '):footnotes_idx].strip()
    else:
        owners_section = "NOT FOUND"

    # Split owners by ';' and extract the owner names (after the '-')
    owners_list = owners_section.split(';') if owners_section != "NOT FOUND" else []
    owners_list = [owner.split('-')[1].strip() for owner in owners_list if '-' in owner]

    # Extract the jockey name and weight (from 'Comments' to 'Fractional Times')
    fractional_start_idx = text_segment.find('Winner:')
    comments_idx = text_segment.find('Comments')

    if comments_idx != -1 and fractional_start_idx != -1 and comments_idx < fractional_start_idx:
        # Extract text block between 'Comments' and 'Winner:' to find jockey and weight info
        jockey_text_block = text_segment[comments_idx + len('Comments'):fractional_start_idx].strip()
        
        # Use regex to capture the jockey name (inside parentheses) and weight (after parentheses)
        jockey_list = re.findall(r'\(([^)]+)\)', jockey_text_block)
        weight_list = re.findall(r'\)\s*(\d+)', jockey_text_block)
    else:
        # If jockey/weight data isn't found, default to empty lists
        jockey_list = []
        weight_list = []

    # Prepare the final list of dictionaries for each horse's data
    horses_data = []

    for i in range(len(pprlp_data)):
        if len(pprlp_data[i]) >= 3:
            # Create a dictionary for each horse, combining PPRLP data, jockey, weight, trainer, and owner
            horse_dict = {
                'program_number': pprlp_data[i][0],  # First value in each PPRLP list is the program number
                'horse_name': pprlp_data[i][1],  # Second value in each PPRLP list is the horse name
                'weight': int(weight_list[i].strip()) if i < len(weight_list) else 'NOT FOUND',  # Extract weight for this horse
                'start_pos': 'N/A' if 'Start' not in PPRLP_text else (pprlp_data[i][2] if pprlp_data[i][2] in ['---', 'N/A'] else int(pprlp_data[i][2])),  # Third value in each PPRLP list is the starting position, Avoid casting '---' to an integer, start_pos is N/A if no start found
                'figures': ', '.join(pprlp_data[i][3:]),  # Remaining values are concatenated into the 'figures' field
                'final_pos': i + 1,  # The final position is based on the order in the PPRLP list
                'jockey': jockey_list[i].strip() if i < len(jockey_list) else 'NOT FOUND',  # Extract jockey name
                'trainer': trainers_list[i].strip() if i < len(trainers_list) else 'NOT FOUND',  # Extract trainer name
                'owner': owners_list[i].strip() if i < len(owners_list) else 'NOT FOUND'  # Extract owner name
            }
            horses_data.append(horse_dict)  # Append the dictionary to the final horse data list

    return horses_data
