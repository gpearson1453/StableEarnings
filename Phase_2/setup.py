import csv
import addData as ad

def process_csv(file_path):
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            # Assuming the CSV contains columns relevant to the methods in addData
            # You can call the appropriate functions here based on the CSV structure

            # Example: if your CSV contains 'name' and 'avg_pos_factor' columns
            if 'entity_type' in row and row['entity_type'] == 'horse':
                ad.addHorse(
                    name=row['name'],
                    avg_pos_factor=row.get('avg_pos_factor'),
                    st_dev_pos_factor=row.get('st_dev_pos_factor'),
                    avg_position_gain=row.get('avg_position_gain'),
                    st_dev_position_gain=row.get('st_dev_position_gain'),
                    avg_late_position_gain=row.get('avg_late_position_gain'),
                    avg_last_position_gain=row.get('avg_last_position_gain'),
                    ewma_perf_factor=row.get('ewma_perf_factor'),
                    most_recent_perf_factor=row.get('most_recent_perf_factor'),
                    ewma_dirt_perf_factor=row.get('ewma_dirt_perf_factor'),
                    ewma_turf_perf_factor=row.get('ewma_turf_perf_factor'),
                    ewma_awt_perf_factor=row.get('ewma_awt_perf_factor'),
                    distance_factor=row.get('distance_factor')
                )
            elif row['entity_type'] == 'track':
                ad.addTrack(
                    name=row['name'],
                    track_speed_factor=row.get('track_speed_factor')
                )
            # Add further conditions based on the entities and relationships you want to process
            # Example for adding Jockey, Trainer, etc.
            elif row['entity_type'] == 'jockey':
                ad.addJockey(
                    name=row['name'],
                    avg_position_gain=row.get('avg_position_gain'),
                    avg_late_position_gain=row.get('avg_late_position_gain'),
                    avg_last_position_gain=row.get('avg_last_position_gain'),
                    total_races=row.get('total_races'),
                    wins=row.get('wins'),
                    places=row.get('places'),
                    shows=row.get('shows'),
                    avg_pos_factor=row.get('avg_pos_factor'),
                    ewma_perf_factor=row.get('ewma_perf_factor')
                )
            # Continue with similar blocks for trainers, owners, races, and relationships

if __name__ == "__main__":
    process_csv('setup.csv')
