from processPPRLP import processPPRLP
import re

def getHorses(text_segment):
    """
    Extracts horse-specific data from a text segment, including Past Performance Running Line Preview (PPRLP) data, 
    trainers, owners, jockeys, and weights. Processes the PPRLP block and combines it with trainer and owner 
    information, returning a list of dictionaries for each horse with all relevant details.

    Parameters:
    - text_segment (str): The full text block from which horse-related data will be extracted.

    Returns:
    - List[Dict]: A list of dictionaries, each containing horse-specific data combined with common race information.
    """

    # Define key phrases to locate sections in the text
    start_phrase = 'Past Performance Running Line Preview'
    end_phrase = 'Trainers:'
    owners_end_phrase = 'Footnotes'

    # Extract the PPRLP section
    start_idx = text_segment.find(start_phrase)
    end_idx = text_segment[start_idx:].find(end_phrase) + start_idx

    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        # Extract and clean PPRLP text
        PPRLP_text = text_segment[start_idx + len(start_phrase):end_idx].strip().replace('\n', ' ')
        pprlp_data = processPPRLP(PPRLP_text)
    else:
        PPRLP_text = "NOT FOUND"
        pprlp_data = []

    # Extract trainer information
    owners_start_idx = text_segment[end_idx:].find('Owners:') + end_idx
    trainers_section = text_segment[end_idx + len('Trainers:'):owners_start_idx].strip() if owners_start_idx != -1 else text_segment[end_idx + len('Trainers:'):].strip()
    trainers_list = [entry.split('-')[1].strip() for entry in trainers_section.split(';') if '-' in entry]

    # Extract owner information
    footnotes_idx = text_segment[owners_start_idx:].find(owners_end_phrase) + owners_start_idx
    if owners_start_idx != -1 and footnotes_idx != -1:
        owners_section = text_segment[owners_start_idx + len('Owners:'):footnotes_idx].strip()
    else:
        owners_section = "NOT FOUND"
    
    owners_list = [entry.split('-')[1].strip() for entry in owners_section.split(';') if '-' in entry and entry.strip()]

    # Extract jockey and weight information
    comments_idx = text_segment.find('Comments')
    fractional_start_idx = text_segment[comments_idx:].find('Winner:') + comments_idx

    if comments_idx != -1 and fractional_start_idx != -1 and comments_idx < fractional_start_idx:
        jockey_text_block = text_segment[comments_idx + len('Comments'):fractional_start_idx].strip()
        jockey_list = re.findall(r'\(([^)]+)\)', jockey_text_block)
        weight_list = re.findall(r'\)\s*(\d+)', jockey_text_block)
    else:
        jockey_list = []
        weight_list = []

    # Initialize list to store horse data
    horses_data = []

    # Construct horse data dictionaries
    for i, entry in enumerate(pprlp_data):
        if len(entry) >= 3:
            horse_dict = {
                'program_number': entry[0],
                'horse_name': entry[1],
                'weight': int(weight_list[i]) if i < len(weight_list) else 'NOT FOUND',
                'start_pos': 'N/A' if 'Start' not in PPRLP_text else (entry[2] if entry[2] in ['---', 'N/A'] else int(entry[2])),
                'figures': ', '.join(entry[2:]) if 'Start' not in PPRLP_text else ', '.join(entry[3:]),
                'final_pos': i + 1,
                'jockey': jockey_list[i].strip() if i < len(jockey_list) else 'NOT FOUND',
                'trainer': trainers_list[i].strip().replace('\n', '') if i < len(trainers_list) else 'NOT FOUND',
                'owner': owners_list[i].strip().replace('\n', '') if i < len(owners_list) else 'NOT FOUND'
            }
            horses_data.append(horse_dict)

    return horses_data
