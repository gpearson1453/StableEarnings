"""
getHorses.py processes horse-specific data extracted from race text segments.

This script defines functions to parse text segments for details about horses participating in races. It extracts
information like program numbers, horse names, jockeys, weights, odds, trainers, owners, and performance figures. It ensures
accurate data extraction even with varied input formats and edge cases like missing or malformed data.

Steps:
    - Extract the Past Performance Running Line Preview (PPRLP) block from the text and call processPPRLP.py to get
      performance line data.
    - Parse the text for jockey, trainer, and owner details.
    - Process performance figures for each horse, including positions and distances.
    - Compile all extracted details into structured dictionaries for each horse.

Functions:
    - processFigs: Processes and formats figures from performance lines.
    - getHorses: Extracts and organizes horse-specific data from a text segment.

Usage:
    Import the getHorses function into scripts that need to process horse-specific data from race text segments. This script
    is not intended to be executed directly.
"""

from processPPRLP import processPPRLP
import re


def processFigs(start, figures, horse_count):
    """
    Process the figures from performance lines into usable data.

    This method converts the extracted text for the performance lines into usable data. Specifically, it returns a list of
    values of length 2n (where n is the number of performance lines in the PPRLP block) where every two values are the horses
    position and distance to the next horse, respectively, for each performance line section. Special cases such as "Nose",
    "Head", and "Neck" are handled with predefined values.

    Args:
        start (str): The starting position for the horse.
        figures (list): A list of figures extracted from the performance lines.
        horse_count (int): Total number of horses in the race.

    Returns:
        list: A list of processed figures where every two values represent
              a horse's position and distance to the next horse.
    """
    result = []
    if horse_count < 10:
        for fig in figures:
            if fig == "---" or fig == "*":
                result.extend(["---", "---"])
            elif "Nose" in fig:
                result.extend([fig[0], "0.05"])
            elif "Head" in fig:
                result.extend([fig[0], "0.2"])
            elif "Neck" in fig:
                result.extend([fig[0], "0.3"])
            elif " " in fig:
                result.extend(
                    [fig[0], str(float(fig[1:-4]) + float(fig[-3]) / float(fig[-1]))]
                )
            elif "/" in fig:
                result.extend([fig[0], str(float(fig[-3]) / float(fig[-1]))])
            else:
                result.extend([fig[0], fig[1:]])
    else:
        prev = start.replace("---", "").replace("*", "")
        for i, fig in enumerate(figures):
            if fig == "---" or fig == "*":
                result.extend(["---", "---"])
            elif "Nose" in fig:
                result.extend([fig[:-4], "0.11275"])
                prev = fig[:-4]
            elif "Head" in fig:
                result.extend([fig[:-4], "0.2255"])
                prev = fig[:-4]
            elif "Neck" in fig:
                result.extend([fig[:-4], "2.125"])
                prev = fig[:-4]
            elif " " in fig:
                if fig[1] == "0":
                    result.extend(
                        [
                            fig[:2],
                            str(float(fig[2:-4]) + float(fig[-3]) / float(fig[-1])),
                        ]
                    )
                    prev = fig[:2]
                elif prev:
                    if (
                        len(fig) == 6
                        or int(fig[:2]) > horse_count
                        or max(int(prev) - int(fig[0]), int(fig[0]) - int(prev))
                        < max(int(prev) - int(fig[:2]), int(fig[:2]) - int(prev))
                    ):
                        result.extend(
                            [
                                fig[0],
                                str(float(fig[1:-4]) + float(fig[-3]) / float(fig[-1])),
                            ]
                        )
                        prev = fig[0]
                    else:
                        result.extend(
                            [
                                fig[:2],
                                str(float(fig[2:-4]) + float(fig[-3]) / float(fig[-1])),
                            ]
                        )
                        prev = fig[:2]
                else:
                    if len(fig) == 6 or int(fig[:2]) > horse_count:
                        result.extend(
                            [
                                fig[0],
                                str(float(fig[1:-4]) + float(fig[-3]) / float(fig[-1])),
                            ]
                        )
                        prev = fig[0]
                    else:
                        result.extend(
                            [
                                fig[:2],
                                str(float(fig[2:-4]) + float(fig[-3]) / float(fig[-1])),
                            ]
                        )
                        prev = fig[:2]
            elif "/" in fig:
                result.extend([fig[:-3], str(float(fig[-3]) / float(fig[-1]))])
                prev = fig[:-3]
            else:
                if fig[1] == "0":
                    result.extend([fig[:2], fig[2:]])
                    prev = fig[:2]
                elif prev:
                    if (
                        len(fig) == 6
                        or int(fig[:2]) > horse_count
                        or max(int(prev) - int(fig[0]), int(fig[0]) - int(prev))
                        < max(int(prev) - int(fig[:2]), int(fig[:2]) - int(prev))
                    ):
                        result.extend([fig[0], fig[1:]])
                        prev = fig[0]
                    else:
                        result.extend([fig[:2], fig[2:]])
                        prev = fig[:2]
                else:
                    if len(fig) == 2 or int(fig[:2]) > horse_count:
                        result.extend([fig[0], fig[1:]])
                        prev = fig[0]
                    else:
                        result.extend([fig[:2], fig[2:]])
                        prev = fig[:2]
    return result


def getHorses(text_segment):
    """
    Extract and process horse-specific data from a text segment.

    Args:
        text_segment (str): The input text segment containing horse details.

    Returns:
        list of dict: A list of dictionaries containing horse-specific data.
    """
    start_phrase = "Past Performance Running Line Preview"
    end_phrase = "Trainers:"
    owners_end_phrase = "Footnotes"

    # Extract the Past Performance Running Line Preview (PPRLP) block
    start_idx = text_segment.find(start_phrase)
    end_idx = text_segment[start_idx:].find(end_phrase) + start_idx

    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        PPRLP_text = (
            text_segment[start_idx + len(start_phrase):end_idx]
            .strip()
            .replace("\n", " ")
        )
        pprlp_data = processPPRLP(PPRLP_text)
    else:
        PPRLP_text = "NOT FOUND"
        pprlp_data = []

    # Extract trainers
    owners_start_idx = text_segment[end_idx:].find("Owners:") + end_idx
    trainers_section = (
        text_segment[end_idx + len("Trainers:"):owners_start_idx].strip()
        if owners_start_idx != -1
        else text_segment[end_idx + len("Trainers:"):].strip()
    )
    trainers_list = [
        entry.split("-")[1].strip()
        for entry in trainers_section.split(";")
        if "-" in entry
    ]

    # Extract owners
    footnotes_idx = (
        text_segment[owners_start_idx:].find(owners_end_phrase) + owners_start_idx
    )
    if owners_start_idx != -1 and footnotes_idx != -1:
        owners_section = text_segment[
            owners_start_idx + len("Owners:"):footnotes_idx
        ].strip()
    else:
        owners_section = "NOT FOUND"

    owners_list = [
        entry.split("-")[1].strip()
        for entry in owners_section.split(";")
        if "-" in entry and entry.strip()
    ]

    # Extract jockeys and weights
    comments_idx = text_segment.find("Comments")
    fractional_start_idx = (
        text_segment[comments_idx:].find("Fractional Times:") + comments_idx
    )
    winner_start_idx = text_segment[comments_idx:].find("Winner:") + comments_idx

    if (
        comments_idx != -1
        and fractional_start_idx != -1
        and comments_idx < fractional_start_idx
    ):
        jockey_text_block = text_segment[
            comments_idx + len("Comments"):fractional_start_idx
        ].strip()
        jockey_list = re.findall(r"\(([^)]+?,[^)]+?)\)", jockey_text_block)
        weight_list = re.findall(r"\)\s*(\d+)", jockey_text_block)
    elif (
        comments_idx != -1
        and winner_start_idx != -1
        and comments_idx < winner_start_idx
    ):
        jockey_text_block = text_segment[
            comments_idx + len("Comments"):winner_start_idx
        ].strip()
        jockey_list = re.findall(r"\(([^)]+?,[^)]+?)\)", jockey_text_block)
        weight_list = re.findall(r"\)\s*(\d+)", jockey_text_block)
    else:
        jockey_list = []
        weight_list = []

    # Extract odds
    horse_name_idx = text_segment.find("Horse Name")
    odds_title_block = (
        comments_idx != -1 and horse_name_idx != -1 and horse_name_idx < comments_idx
    )
    if odds_title_block:
        odds_found = "Odds" in text_segment[horse_name_idx:comments_idx]
        odds = []
        for j in jockey_list:
            odds_match = re.search(
                r"\b\d+\.\d+\b", jockey_text_block[jockey_text_block.find(j):]
            )
            odds.append(odds_match.group() if odds_found and odds_match else "N/A")

    # Compile horse-specific data
    horses_data = []

    for i, entry in enumerate(pprlp_data):
        if len(entry) >= 3:
            horse_dict = {
                "program_number": entry[0],
                "horse_name": entry[1],
                "odds": odds[i].strip() if odds_title_block else "NOT FOUND",
                "weight": int(weight_list[i]) if i < len(weight_list) else "NOT FOUND",
                "start_pos": (
                    "N/A"
                    if "Start"
                    not in PPRLP_text[
                        PPRLP_text.find("Pgm"):PPRLP_text.find("Pgm") + 25
                    ]
                    else (entry[2] if entry[2] in ["---", "N/A"] else int(entry[2]))
                ),
                "figures": (
                    ", ".join(processFigs("", entry[2:], len(pprlp_data)))
                    if "Start" not in PPRLP_text[: PPRLP_text.find("Horse") + 20]
                    else ", ".join(processFigs(entry[2], entry[3:], len(pprlp_data)))
                ),
                "final_pos": i + 1,
                "total_horses": len(pprlp_data),
                "jockey": (
                    jockey_list[i].strip() if i < len(jockey_list) else "NOT FOUND"
                ),
                "trainer": (
                    trainers_list[i].strip().replace("\n", "")
                    if i < len(trainers_list)
                    else "NOT FOUND"
                ),
                "owner": (
                    owners_list[i].strip().replace("\n", "")
                    if i < len(owners_list)
                    else "NOT FOUND"
                ),
            }
            horses_data.append(horse_dict)
    return horses_data
