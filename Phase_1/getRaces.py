"""
getRaces.py extracts and processes race-related data from a text segment.

This script defines functions to process a text segment containing race information. It extracts metadata
such as race details, weather, surface, times, and more. It also integrates horse-specific data by calling
the getHorses function from getHorses.py and calculates positional factors for horses based on their performance.

Steps:
    - Parse the input text segment to extract general race information (e.g., date, location, weather).
    - Identify and process specific data such as fractional and split times, surface type, and distance.
    - Validate the race type to ensure it is supported (e.g., Thoroughbred or Quarter Horse).
    - Call the getHorses function to retrieve horse-specific data.
    - Calculate positional factors for horses based on their relative performance.
    - Combine common race data with horse-specific data into a structured output.

Functions:
    - remove_weird_chars: Cleans up a string by removing unwanted characters.
    - calculatePosFactors: Calculates positional factors for horses based on their performance.
    - getRaces: Main function to extract race data from a text segment and combine it with horse data.

Usage:
    Import the getRaces function into other scripts to extract structured race data from text segments. This script is not
    intended to be executed directly.
"""

import re
from mappings import TYPE_PATTERNS, DISTANCE_CONVERSION
from getHorses import getHorses
import statistics as stats


def remove_weird_chars(string):
    """
    Remove unwanted characters from the given string.

    Args:
        string (str): The input string to be cleaned.

    Returns:
        str: The cleaned string with unwanted characters removed.
    """
    translation_table = str.maketrans("", "", "<*?/[\n\r]")
    return string.translate(translation_table)


def calculatePosFactors(figs):
    """
    Calculate positional factors for horses based on their performance gaps.

    This method calculates a position factor based on horses' final positions relative to each other. First, each
    horse's gap to each other horse is calculated. A positive value indicates leading another horse, and a negative
    value indicates trailing another horse. Then, for each horse, the average of all of the gaps to the other horses is
    calculated. Finally, to reduce the skewing effect of horses trailing the pack  by large margins, the z-score of
    each average gap is calculated by dividing the average gap by the standard deviation of all average gap values. The
    function returns a list of these average gap z-score values.

    Args:
        figs (list of str): A list of figures (e.g., gap times) representing horse performance.

    Returns:
        list of float: A list of positional factor z-scores for each horse.
    """
    figs_length = len(figs)
    figs[0] = 0  # Anchor the first figure to 0 for relative calculations
    arr = [[0] * figs_length for _ in range(figs_length)]
    for i in range(figs_length):
        if i == 0:
            for j in range(1, figs_length):
                arr[i][j] = arr[i][j - 1] + float(figs[j])
        else:
            for j in range(figs_length):
                arr[i][j] = arr[i - 1][j] - float(figs[i])
    means = [stats.mean(list) * figs_length / (figs_length - 1) for list in arr]
    sd = stats.stdev(means)
    return [m / sd for m in means]


def getRaces(text_segment):
    """
    Extract and process race-related data from a text segment.

    Args:
        text_segment (str): The input text segment containing race details.

    Returns:
        list of dict: A list of dictionaries combining race and horse-specific data.
    """
    # Initialize common race data with default values
    common_data = {
        "race_number": "NOT FOUND",
        "date": "NOT FOUND",
        "location": "NOT FOUND",
        "race_description": "NOT FOUND",
        "race_type": "NOT FOUND",
        "surface": "NOT FOUND",
        "weather": "NOT FOUND",
        "temp": "NOT FOUND",
        "track_state": "NOT FOUND",
        "distance(miles)": "NOT FOUND",
        "fractional_a": "NOT FOUND",
        "fractional_b": "NOT FOUND",
        "fractional_c": "NOT FOUND",
        "fractional_d": "NOT FOUND",
        "fractional_e": "NOT FOUND",
        "fractional_f": "NOT FOUND",
        "final_time": "NOT FOUND",
        "split_a": "NOT FOUND",
        "split_b": "NOT FOUND",
        "split_c": "NOT FOUND",
        "split_d": "NOT FOUND",
        "split_e": "NOT FOUND",
        "split_f": "NOT FOUND",
    }

    # Remove 'Inner' and 'Outer' from track surface text
    if text_segment.find("Distance:"):
        text_segment = text_segment[: text_segment.find("Distance:")] + text_segment[
            text_segment.find("Distance:"):
        ].replace("Inner", "", 1).replace("Outer", "", 1)

    # Extract fractional times
    if "Fractional Times:" in text_segment:
        frac_times_text = (
            text_segment.split("Fractional Times:")[1].split("Final Time:")[0].strip()
        )
        frac_times = frac_times_text.split()
        for i, key in enumerate(
            [
                "fractional_a",
                "fractional_b",
                "fractional_c",
                "fractional_d",
                "fractional_e",
                "fractional_f",
            ]
        ):
            common_data[key] = frac_times[i] if i < len(frac_times) else "N/A"
    else:
        for key in [
            "fractional_a",
            "fractional_b",
            "fractional_c",
            "fractional_d",
            "fractional_e",
            "fractional_f",
        ]:
            common_data[key] = "N/A"

    # Extract split times
    if "Split Times:" in text_segment:
        split_times_text = (
            text_segment.split("Split Times:")[1].split("Run-Up:")[0].strip()
        )
        split_times = split_times_text.split()
        for i, key in enumerate(
            ["split_a", "split_b", "split_c", "split_d", "split_e", "split_f"]
        ):
            common_data[key] = (
                split_times[i].replace("(", "").replace(")", "")
                if i < len(split_times)
                else "N/A"
            )
    else:
        for key in ["split_a", "split_b", "split_c", "split_d", "split_e", "split_f"]:
            common_data[key] = "N/A"

    # Extract final time
    if "Final Time:" in text_segment:
        final_time_match = re.search(r"Final Time:\s(.*)\s", text_segment)
        common_data["final_time"] = (
            final_time_match.group(1).replace("(New Track Record)", "")
            if final_time_match
            else "N/A"
        )
    else:
        common_data["final_time"] = "N/A"

    # Extract race number
    race_match = re.search(r"Race\s*(\d+)", text_segment)
    common_data["race_number"] = int(race_match.group(1)) if race_match else "NOT FOUND"

    # Extract date
    date_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})",
        text_segment,
    )
    if date_match:
        month, day, year = date_match.groups()
        common_data["date"] = f"{month} {day}, {year}"

    # Extract location
    if " - " in text_segment:
        location = remove_weird_chars(text_segment.split(" - ")[0].replace("\n", ""))
        common_data["location"] = location

    # Generate race description
    if all(
        [
            common_data["date"],
            common_data["location"],
            common_data["race_number"] != "NOT FOUND",
        ]
    ):
        common_data["race_description"] = (
            f"{common_data['date']}_{common_data['location']}_{common_data['race_number']}"
        )

    # Extract distance and surface
    distance_surface_match = re.search(
        r"Distance:\s*(.*?)\s*On The\s*(\S+)", text_segment
    )
    if distance_surface_match:
        distance_idx = text_segment.find("Distance:")
        surface_segment = text_segment[distance_idx:distance_idx + 80].lower()
        raw_distance, raw_surface = distance_surface_match.groups()
        common_data["distance(miles)"] = DISTANCE_CONVERSION.get(
            raw_distance.replace("About", "").strip(), "NOT FOUND"
        )
        if "dirt" in surface_segment:
            common_data["surface"] = "Dirt"
        elif "turf" in surface_segment:
            common_data["surface"] = "Turf"
        elif "all weather" in surface_segment:
            common_data["surface"] = "AWT"
        elif "hurdle" in surface_segment:
            common_data["surface"] = "Hurdle"
        elif "timber" in surface_segment:
            common_data["surface"] = "Timber"
        else:
            print("problem with " + common_data["race_id"])

    # Extract weather and temperature
    if "° C" in text_segment:
        weather_match = re.search(r"Weather:\s*(.*?),\s*(-*\d+°*\sC*)", text_segment)
    else:
        weather_match = re.search(r"Weather:\s*(.*?),\s*(\d+)", text_segment)
    if weather_match:
        common_data["weather"] = weather_match.group(1).strip()
        common_data["temp"] = weather_match.group(2).strip()

    # Extract track state
    track_state_match = re.search(r"Track:\s*(\S+)", text_segment)
    if track_state_match:
        common_data["track_state"] = track_state_match.group(1).strip()

    # Extract race type
    lines = text_segment.split("\n")
    line = lines[1] + lines[2]
    line = line.replace("\n", "")
    for type in TYPE_PATTERNS:
        if type in line:
            common_data["race_type"] = type.replace(" - ", "")
            break
    if common_data["race_type"] not in [
        "Thoroughbred",
        "Quarter Horse",
        "Mixed",
        "Arabian",
    ]:
        return "Invalid Race Type"

    # Retrieve individual horse data using getHorses
    horse_data_list = getHorses(text_segment)

    if len(horse_data_list) < 2:
        return

    # Integrate horse data and calculate positional factors
    final_figs = [
        horse_data["figures"].split(", ")[-1] for horse_data in horse_data_list
    ]
    while final_figs[-1] == "---":
        final_figs = final_figs[:-1]
    pos_factors = calculatePosFactors(final_figs) if len(final_figs) > 1 else []

    # Combine common data with horse-specific data
    combined_data = [
        {
            **common_data,
            **{
                **horse_data,
                **{"pos_factor": "" if i >= len(pos_factors) else pos_factors[i]},
            },
        }
        for i, horse_data in enumerate(horse_data_list)
    ]

    return combined_data
