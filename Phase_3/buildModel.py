"""
creating batches:
    max batch size of 32
    each batch should have the same number of horses per race
    the batches list will be a list of tensors with dimensions (batch_size, num_horses, num_features - 4)
        4 features are not being trained with: race_id, final_pos, horse_n_name, race_type

heres the features that will be trained on (87):
    "track_ewma_speed"
    "weight"
    "horse_total_races"
    "horse_wins"
    "horse_places"
    "horse_shows"
    "horse_ewma_pos_factor"
    "horse_ewma_pos_gain"
    "horse_ewma_late_pos_gain"
    "horse_ewma_last_pos_gain"
    "horse_perf_factor_count"
    "horse_ewma_perf_factor"
    "horse_recent_perf_factor"
    "horse_ewma_surface_perf_factor"
    "horse_distance_factor"
    "jockey_ewma_pos_gain"
    "jockey_ewma_late_pos_gain"
    "jockey_ewma_last_pos_gain"
    "jockey_total_races"
    "jockey_wins"
    "jockey_places"
    "jockey_shows"
    "jockey_ewma_pos_factor"
    "jockey_perf_factor_count"
    "jockey_ewma_perf_factor"
    "trainer_total_races"
    "trainer_wins"
    "trainer_places"
    "trainer_shows"
    "trainer_ewma_pos_factor"
    "trainer_perf_factor_count"
    "trainer_ewma_perf_factor"
    "trainer_ewma_surface_perf_factor"
    "trainer_distance_factor"
    "owner_total_races"
    "owner_wins"
    "owner_places"
    "owner_shows"
    "owner_ewma_pos_factor"
    "owner_perf_factor_count"
    "owner_ewma_perf_factor"
    "weather"
    "temperature"
    "track_state"
    "distance"
    "horse_jockey_total_races"
    "horse_jockey_wins"
    "horse_jockey_places"
    "horse_jockey_shows"
    "horse_jockey_ewma_pos_factor"
    "horse_jockey_perf_factor_count"
    "horse_jockey_ewma_perf_factor"
    "horse_trainer_total_races"
    "horse_trainer_wins"
    "horse_trainer_places"
    "horse_trainer_shows"
    "horse_trainer_ewma_pos_factor"
    "horse_trainer_perf_factor_count"
    "horse_trainer_ewma_perf_factor"
    "trainer_track_total_races"
    "trainer_track_wins"
    "trainer_track_places"
    "trainer_track_shows"
    "trainer_track_ewma_pos_factor"
    "trainer_track_perf_factor_count"
    "trainer_track_ewma_perf_factor"
    "owner_trainer_total_races"
    "owner_trainer_wins"
    "owner_trainer_places"
    "owner_trainer_shows"
    "owner_trainer_ewma_pos_factor"
    "owner_trainer_perf_factor_count"
    "owner_trainer_ewma_perf_factor"
    "horse_track_total_races"
    "horse_track_wins"
    "horse_track_places"
    "horse_track_shows"
    "horse_track_ewma_pos_factor"
    "horse_track_perf_factor_count"
    "horse_track_ewma_perf_factor"
    "jockey_trainer_total_races"
    "jockey_trainer_wins"
    "jockey_trainer_places"
    "jockey_trainer_shows"
    "jockey_trainer_ewma_pos_factor"
    "jockey_trainer_perf_factor_count"
    "jockey_trainer_ewma_perf_factor"

embedding layer:
    reduce dimensionality from 87 down to 64
"""

from sqlalchemy import create_engine
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import random

batches = []


class RacePredictor(nn.Module):
    def __init__(self, input_dim, embed_dim, w_input_dim, w_embed_dim, ts_input_dim, ts_embed_dim, num_heads, output_dim=3):
        super(RacePredictor, self).__init__()
        self.embedding = nn.Linear(input_dim, embed_dim)  # Transform raw features into embeddings
        self.weather_embedding = nn.Embedding(w_input_dim, w_embed_dim, padding_idx=0)  # weather embedding
        self.track_state_embedding = nn.Embedding(ts_input_dim, ts_embed_dim, padding_idx=0)  # track_state_embedding
        self.attention = nn.MultiheadAttention(embed_dim + w_embed_dim + ts_embed_dim, num_heads, batch_first=True)
        self.fc = nn.Linear(embed_dim + w_embed_dim + ts_embed_dim, output_dim)  # Predict 3 probabilities for each car
        self.softmax = nn.Softmax(dim=-1)  # Ensure probabilities sum to 1 for each car

    def forward(self, x_num, x_weather, x_track_state):
        """
        Forward pass for the RacePredictor model.

        Args:
        x_num (torch.Tensor): Numerical features of shape (batch_size, num_cars, input_dim).
        x_weather (torch.Tensor): Weather categorical values of shape (batch_size, num_cars).
        x_track_state (torch.Tensor): Track state categorical values of shape (batch_size, num_cars).

        Returns:
        torch.Tensor: Probabilities for each car of shape (batch_size, num_cars, output_dim).
        """
        # Embed numerical features
        x_num_emb = self.embedding(x_num)  # (batch_size, num_cars, embed_dim)

        # Embed categorical variables
        x_weather_emb = self.weather_embedding(x_weather)  # (batch_size, num_cars, weather_embed_dim)
        x_track_state_emb = self.track_state_embedding(x_track_state)  # (batch_size, num_cars, track_state_embed_dim)
        
        # Concatenate all features
        x = torch.cat([x_num_emb, x_weather_emb, x_track_state_emb], dim=-1)  # (batch_size, num_cars, combined_embed_dim)
        
        # Apply multihead attention
        attn_output, _ = self.attention(x, x, x)  # Self-attention: Q = K = V = x
        
        # Predict probabilities
        logits = self.fc(attn_output)  # (batch_size, num_cars, output_dim)
        
        return logits  # (batch_size, num_cars, output_dim)


def local_connect(db_name):
    """
    Establish a connection to a PostgreSQL database using SQLAlchemy.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        sqlalchemy.engine.base.Engine: An SQLAlchemy engine object for the database.
    """
    return create_engine(f"postgresql+psycopg2://postgres:B!h8Cjxa37!78Yh@localhost:5432/{db_name}")


def getWeatherTrackStateDims():
    engine = local_connect("StableEarnings")

    query = """
        SELECT max(weather) as max_w, max(track_state) as max_ts
        FROM trainables;
    """
    df = pd.read_sql_query(query, engine)

    engine.dispose()

    return int(df['max_w'].iloc[0]) + 1, int(df['max_ts'].iloc[0]) + 1


def buildBatches(batch_size):
    global batches

    engine = local_connect("StableEarnings")

    query = """
        SELECT *
        FROM trainables
        ORDER BY race_id;
    """
    df = pd.read_sql_query(query, engine)

    engine.dispose()

    grouped = df.groupby('race_id')

    group_numpys = [data_frame.to_numpy() for race_id, data_frame in grouped]

    batch_maker = {}

    for arr in group_numpys:
        arr_len = len(arr)
        if arr_len < 4:
            continue
        if arr_len in batch_maker:
            batch_maker[arr_len] = np.append(batch_maker[arr_len], [arr], axis=0)
            if len(batch_maker[arr_len]) == batch_size:
                batches.append(torch.from_numpy(batch_maker.pop(arr_len)[:, :, 4:].astype(np.float32)).float())
        else:
            batch_maker[arr_len] = np.array([arr])

    for arr_len in list(batch_maker.keys()):
        batches.append(torch.from_numpy(batch_maker.pop(arr_len)[:, :, 4:].astype(np.float32)).float())


# Define a training function
def train_model(model, batches, num_epochs=10, learning_rate=0.001):
    """
    Train the RacePredictor model.

    Args:
        model (nn.Module): The RacePredictor model to train.
        batches (list): List of training batches, where each batch is a list of DataFrames.
        num_epochs (int): Number of epochs to train the model.
        learning_rate (float): Learning rate for the optimizer.
    """
    # Define loss function and optimizer
    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    # Iterate over epochs
    for epoch in range(num_epochs):
        model.train()  # Set model to training mode
        epoch_loss = 0.0

        # Randomize batch order
        random.shuffle(batches)

        for batch in batches:
            # Prepare the data
            features = torch.cat(
                (
                    batch[:, :, :41],
                    batch[:, :, 42:43],
                    batch[:, :, 44:-3]
                ), dim=2)  # Prepare the data
            missing_mask = torch.isnan(features).float()  # 1 for NaN, 0 for valid values
            data_filled = torch.nan_to_num(features, nan=0.0)  # fills NaNs with a placeholder
            data_with_mask = torch.cat([data_filled, missing_mask], dim=-1)  # concatenates data with mask
            weather = batch[:, :, 41].long()  # Weather as categorical
            track_state = batch[:, :, 43].long()  # Weather as categorical
            targets = batch[:, :, -3:]  # Labels

            # Zero the gradients
            optimizer.zero_grad()

            # Forward pass
            outputs = model(data_with_mask, weather, track_state)
            
            # Compute the loss
            loss = criterion(outputs, targets)

            # Backward pass and optimization
            loss.backward()
            optimizer.step()

            # Accumulate loss for reporting
            epoch_loss += loss.item()

        # Print epoch loss
        print(f"Epoch {epoch + 1}/{num_epochs}, Loss: {epoch_loss / len(batches)}")


if __name__ == "__main__":
    # Build batches
    buildBatches(32)

    w_input_dim, ts_input_dim = getWeatherTrackStateDims()

    # Initialize the model
    input_dim = 170  # Number of input features ((87 features - 2 categoricals) * 2 for masking)
    embed_dim = 54  # Embedding dimension
    w_embed_dim = 4  # weather embedding dimension
    ts_embed_dim = 6  # track_state embedding dimension
    num_heads = 8   # Number of attention heads
    model = RacePredictor(input_dim, embed_dim, w_input_dim, w_embed_dim, ts_input_dim, ts_embed_dim, num_heads)

    # Train the model
    train_model(model, batches, num_epochs=20, learning_rate=0.001)

    torch.save(model.state_dict(), "race_predictor.pth")
    print("Model saved as race_predictor_model.pth")
