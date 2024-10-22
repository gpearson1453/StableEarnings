import re
from mappings import TYPE_PATTERNS, DISTANCE_CONVERSION, SURFACE_MAPPING
from getHorses import getHorses
import uuid

def remove_weird_chars(string):
    """
    Remove specific unwanted characters like newlines and certain symbols from a string.

    Parameters:
    - string (str): The input string to be cleaned.

    Returns:
    - str: A cleaned string with unwanted characters removed.
    """
    translation_table = str.maketrans('', '', '<*?/[\n\r]')
    return string.translate(translation_table)

def getRaces(text_segment):
    """
    Extract race-specific data from a text segment, including race number, date, location,
    distance, surface type, weather, and track conditions. This function also calls getHorses 
    to extract horse-specific data for each race and combines it with the common race data.

    Parameters:
    - text_segment (str): The full text block containing race information.

    Returns:
    - List[Dict]: A list of dictionaries where each dictionary contains common race data 
      combined with horse-specific data. Returns 'Invalid Race Type' if the race type is not valid.
    """

    # Initialize dictionary for common race-wide data
    common_data = {
        'race_number': "NOT FOUND",
        'date': "NOT FOUND",
        'location': "NOT FOUND",
        'race_id': "NOT FOUND",
        'race_type': "NOT FOUND",
        'surface': "NOT FOUND",
        'weather': "NOT FOUND",
        'temp': "NOT FOUND",
        'track_state': "NOT FOUND",
        'distance(miles)': "NOT FOUND",
        'fractional_a': "NOT FOUND",
        'fractional_b': "NOT FOUND",
        'fractional_c': "NOT FOUND",
        'fractional_d': "NOT FOUND",
        'fractional_e': "NOT FOUND",
        'fractional_f': "NOT FOUND",
        'final_time': "NOT FOUND",
        'split_a': "NOT FOUND",
        'split_b': "NOT FOUND",
        'split_c': "NOT FOUND",
        'split_d': "NOT FOUND",
        'split_e': "NOT FOUND",
        'split_f': "NOT FOUND",
        'unique_race_id': str(uuid.uuid4())
    }

    # this seciton will remove 'Inner' and 'Outer' from the surface segment to allow for correct mapping
    if text_segment.find('Distance:'):
        text_segment = text_segment[:text_segment.find('Distance:')] + text_segment[text_segment.find('Distance:'):].replace('Inner', '', 1).replace('Outer', '', 1)

    # Extract fractional times
    if 'Fractional Times:' in text_segment:
        frac_times_text = text_segment.split('Fractional Times:')[1].split('Final Time:')[0].strip()
        frac_times = frac_times_text.split()
        for i, key in enumerate(['fractional_a', 'fractional_b', 'fractional_c', 'fractional_d', 'fractional_e', 'fractional_f']):
            common_data[key] = frac_times[i] if i < len(frac_times) else 'N/A'
    else:
        for key in ['fractional_a', 'fractional_b', 'fractional_c', 'fractional_d', 'fractional_e', 'fractional_f']:
            common_data[key] = 'N/A'

    # Extract split times
    if 'Split Times:' in text_segment:
        split_times_text = text_segment.split('Split Times:')[1].split('Run-Up:')[0].strip()
        split_times = split_times_text.split()
        for i, key in enumerate(['split_a', 'split_b', 'split_c', 'split_d', 'split_e', 'split_f']):
            common_data[key] = split_times[i].replace('(', '').replace(')', '') if i < len(split_times) else 'N/A'
    else:
        for key in ['split_a', 'split_b', 'split_c', 'split_d', 'split_e', 'split_f']:
            common_data[key] = 'N/A'

    # Extract final time
    if 'Final Time:' in text_segment:
        final_time_match = re.search(r"Final Time:\s(.*)\s", text_segment)
        common_data['final_time'] = final_time_match.group(1).replace('(New Track Record)', '') if final_time_match else 'N/A'
    else:
        common_data['final_time'] = 'N/A'

    # Extract race number
    race_match = re.search(r"Race\s*(\d+)", text_segment)
    common_data['race_number'] = int(race_match.group(1)) if race_match else "NOT FOUND"

    # Extract date
    date_match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})", text_segment)
    if date_match:
        month, day, year = date_match.groups()
        common_data['date'] = f"{month} {day}, {year}"

    # Extract location
    if ' - ' in text_segment:
        location = remove_weird_chars(text_segment.split(' - ')[0].replace('\n', ''))
        common_data['location'] = location

    # Generate race ID if possible
    if all([common_data['date'], common_data['location'], common_data['race_number'] != "NOT FOUND"]):
        common_data['race_id'] = f"{common_data['date']}_{common_data['location']}_{common_data['race_number']}"

    # Extract distance and surface
    distance_surface_match = re.search(r"Distance:\s*(.*?)\s*On The\s*(\S+)", text_segment)
    if distance_surface_match:
        raw_distance, raw_surface = distance_surface_match.groups()
        common_data['distance(miles)'] = DISTANCE_CONVERSION.get(raw_distance.replace("About", '').strip(), "NOT FOUND")
        common_data['surface'] = SURFACE_MAPPING.get(raw_surface.strip()[0], "NOT FOUND")

    # Extract weather and temperature
    if '° C' in text_segment:
        weather_match = re.search(r"Weather:\s*(.*?),\s*(-*\d+°*\sC*)", text_segment)
    else: 
        weather_match = re.search(r"Weather:\s*(.*?),\s*(\d+)", text_segment)
    if weather_match:
        common_data['weather'] = weather_match.group(1).strip()
        common_data['temp'] = weather_match.group(2).strip()

    # Extract track state
    track_state_match = re.search(r"Track:\s*(\S+)", text_segment)
    if track_state_match:
        common_data['track_state'] = track_state_match.group(1).strip()

    # Validate race type
    lines = text_segment.split('\n')
    line = lines[1] + lines[2]
    line = line.replace('\n', '')
    for type in TYPE_PATTERNS:
        if type in line:
            common_data['race_type'] = type.replace(' - ', '')
            break
    if common_data['race_type'] not in ["Thoroughbred", 'Quarter Horse', 'Mixed', 'Arabian']:
        return 'Invalid Race Type'

    # Extract horse-specific data
    horse_data_list = getHorses(text_segment)

    # Combine common race data with horse data
    combined_data = [{**common_data, **horse_data} for horse_data in horse_data_list]

    return combined_data
