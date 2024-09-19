import re
from mappings import TYPE_PATTERNS, DISTANCE_CONVERSION, SURFACE_MAPPING
from getHorses import getHorses  # Import getHorses to extract horse-specific data

def remove_weird_chars(string):
    """
    Remove specific unwanted characters such as newlines and symbols from a string.
    
    Parameters:
    - string: The input string to be cleaned.
    
    Returns:
    - A cleaned string with specific characters removed.
    """
    translation_table = str.maketrans('', '', '<*?/[\n\r]')
    return string.translate(translation_table)

def getRaces(text_segment):
    """
    Extract race-specific data from a text segment, including race number, date, location, 
    distance, surface type, and weather conditions. This function also calls getHorses 
    to extract horse-specific data for each race and combines it with the common race data.
    
    Parameters:
    - text_segment: A string containing the full text block of the race, from which data 
      will be extracted.
    
    Returns:
    - A list of dictionaries, where each dictionary contains both common race data and 
      individual horse data. Returns 'Invalid Race Type' if the race type is not valid.
    """

    # Initialize a dictionary to store race-wide data that will be shared with each horse's entry
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
        'split_f': "NOT FOUND"
    }

    # Define regex patterns for extracting specific race information
    race_number_pattern = r"Race\s*(\d+)"  # Pattern for extracting race number in format 'Race #'
    date_pattern = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})"  # Pattern for extracting date in 'Month Day, Year' format
    distance_surface_pattern = r"Distance:\s*(.*?)\s*On The\s*(\S+)"  # Pattern for extracting distance and surface
    weather_pattern = r"Weather:\s*(.*?),\s*(\d+)"  # Pattern for extracting weather and temperature
    track_state_pattern = r"Track:\s*(\S+)"  # Pattern for extracting track state

    # Extract fractional times if they are present in the text
    if 'Fractional Times:' in text_segment:
        frac_times_text = text_segment[text_segment.find('Fractional Times:') + len('Fractional Times:'): text_segment.find('Final Time:') - 1].strip()
        frac_times = frac_times_text.split()
        for key in ['fractional_a', 'fractional_b', 'fractional_c', 'fractional_d', 'fractional_e', 'fractional_f']:
            if len(frac_times) > 0:
                common_data[key] = frac_times.pop(0)
            else:
                common_data[key] = 'N/A'
    else:
        for key in ['fractional_a', 'fractional_b', 'fractional_c', 'fractional_d', 'fractional_e', 'fractional_f']:
            common_data[key] = 'N/A'

    # Extract split times if they are present in the text
    if 'Split Times:' in text_segment:
        split_times_text = text_segment[text_segment.find('Split Times:') + len('Split Times:'): text_segment.find('Run-Up:') - 1].strip()
        split_times = split_times_text.split()
        for key in ['split_a', 'split_b', 'split_c', 'split_d', 'split_e', 'split_f']:
            if len(split_times) > 0:
                common_data[key] = split_times.pop(0).replace('(', '').replace(')', '')
            else:
                common_data[key] = 'N/A'
    else:
        for key in ['split_a', 'split_b', 'split_c', 'split_d', 'split_e', 'split_f']:
            common_data[key] = 'N/A'

    # Extract the final time for the race
    if 'Final Time:' in text_segment:
        final_time_pattern = r"Final Time:\s(.*)\s"
        final_time_match = re.search(final_time_pattern, text_segment)
        if final_time_match:
            common_data['final_time'] = final_time_match.group(1)
    else:
        common_data['final_time'] = 'N/A'

    # Extract the race number
    race_match = re.search(race_number_pattern, text_segment)
    if race_match:
        try:
            common_data['race_number'] = int(race_match.group(1))
        except ValueError:
            common_data['race_number'] = "NOT FOUND"

    # Extract the date
    date_match = re.search(date_pattern, text_segment)
    if date_match:
        month, day, year = date_match.groups()
        common_data['date'] = f"{month} {day}, {year}"

    # Extract the location (assumes the location is the first part of the segment followed by ' - ')
    if ' - ' in text_segment:
        location = remove_weird_chars(text_segment[:text_segment.find(' - ')].replace('\n', ''))
        common_data['location'] = location

    # Combine the date, location, and race number to create a unique race ID
    if common_data['date'] != "NOT FOUND" and common_data['location'] != "NOT FOUND" and common_data['race_number'] != "NOT FOUND":
        common_data['race_id'] = f"{common_data['date']}_{common_data['location']}_{common_data['race_number']}"

    # Extract the distance and surface type
    distance_surface_match = re.search(distance_surface_pattern, text_segment)
    if distance_surface_match:
        raw_distance = distance_surface_match.group(1).strip()
        raw_surface = distance_surface_match.group(2).strip()
        try:
            common_data['distance(miles)'] = float(DISTANCE_CONVERSION.get(raw_distance, "NOT FOUND"))
        except ValueError:
            common_data['distance(miles)'] = "NOT FOUND"
        common_data['surface'] = SURFACE_MAPPING.get(raw_surface[0], "NOT FOUND")

    # Extract the weather and temperature
    weather_match = re.search(weather_pattern, text_segment)
    if weather_match:
        common_data['weather'] = weather_match.group(1).strip()
        try:
            common_data['temp'] = float(weather_match.group(2).strip())
        except ValueError:
            common_data['temp'] = "NOT FOUND"

    # Extract the track state
    track_state_match = re.search(track_state_pattern, text_segment)
    if track_state_match:
        common_data['track_state'] = track_state_match.group(1).strip()

    # Identify race type (if not Thoroughbred or Quarter Horse, return 'Invalid Race Type')
    lines = text_segment.split('\n')
    for i in range(1, min(3, len(lines))):  # Check second and third lines for race type
        line = lines[i].strip()
        for race_type_key, pattern in TYPE_PATTERNS.items():
            if pattern.search(line):
                common_data['race_type'] = race_type_key
                break
    if common_data['race_type'] not in ["Thoroughbred", 'Quarter Horse']:
        return 'Invalid Race Type'

    # Call getHorses to extract horse-specific data for the race
    horse_data_list = getHorses(text_segment)

    # Combine the common race data with individual horse data
    combined_data = []
    for horse_data in horse_data_list:
        combined_entry = {**common_data, **horse_data}  # Merge race data and horse data
        combined_data.append(combined_entry)

    return combined_data
