import re
import json
import os

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Path to the JSON file
MAPPINGS_FILE = 'mappings.json'

def load_mappings():
    """
    Load mappings from a JSON file, specifically for 'stupid_horse_names' and 'fixed' mappings.

    Returns:
    - dict: A dictionary containing the mappings, or default empty dictionaries if the file doesn't exist.
    """
    if os.path.exists(MAPPINGS_FILE):
        with open(MAPPINGS_FILE, 'r') as file:
            return json.load(file)
    return {"stupid_horse_names": {}, "fixed": {}}

def save_mappings(mappings):
    """
    Save the current mappings to the JSON file.

    Parameters:
    - mappings (dict): The mappings dictionary to be saved.
    """
    with open(MAPPINGS_FILE, 'w') as file:
        json.dump(mappings, file, indent=4)

# Load mappings at the start of the program
mappings = load_mappings()
stupid_horse_names = mappings.get('stupid_horse_names', {})
fixed = mappings.get('fixed', {})

# Distance conversion mappings from race distance descriptions to miles
DISTANCE_CONVERSION = {
    "Six Furlongs": "0.75",
    "One Mile": "1.0",
    "Two Miles": "2.0",
    "Six And One Half Furlongs": "0.8125",
    "Five And One Half Furlongs": "0.6875",
    "One And One Sixteenth Miles": "1.0625",
    "Seven Furlongs": "0.875",
    "Five Furlongs": "0.625",
    "One Mile And Seventy Yards": "1.0397727",
    "Four And One Half Furlongs": "0.5625",
    "Three Hundred And Fifty Yards": "0.198864",
    "Four Hundred Yards": "0.227273",
    "Three Hundred Yards": "0.170455",
    "One And One Eighth Miles": "1.125",
    "One And Five Eighth Miles": "1.625",
    "One And Three Eighth Miles": "1.375",
    "One And Five Sixteenth Miles": "1.3125",
    "Eight Hundred And Seventy Yards": "0.494318",
    "One Mile And Forty Yards": "1.0227273",
    "About One Mile": "1.0",
    "Three Hundred And Thirty Yards": "0.1875",
    "Two Hundred And Twenty Yards": "0.125",
    "Two Hundred And Fifty Yards": "0.142045",
    "About Seven And One Half Furlongs": "0.9375",
    "Four Hundred And Forty Yards": "0.25",
    "Seven And One Half Furlongs": "0.9375",
    "About Five And One Half Furlongs": "0.6875",
    "About One And One Sixteenth Miles": "1.0625",
    "One Hundred And Ten Yards": "0.0625",
    "One And One Half Miles": "1.5",
    "One And Three Sixteenth Miles": "1.1875",
    "Two And One Half Furlongs": "0.3125",
    "Four Furlongs": "0.5",
    "Five Hundred And Fifty Yards": "0.3125",
    "About One And One Eighth Miles": "1.125",
    "Two Furlongs": "0.25",
    "One And One Fourth Miles": "1.25"
}

# Surface type mappings for races
SURFACE_MAPPING = {
    'D': 'Dirt',
    'T': 'Turf',
    'A': 'AWT'
}

# Regular expressions to identify different race types
TYPE_PATTERNS = {
    'Thoroughbred': re.compile(r'thoroughbred', re.IGNORECASE),
    'Quarter Horse': re.compile(r'quarter horse', re.IGNORECASE),
    'Mixed': re.compile(r'mixed', re.IGNORECASE),
    'Arabian': re.compile(r'arabian', re.IGNORECASE),
}
