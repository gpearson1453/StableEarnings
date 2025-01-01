from buildModel import local_connect, getWeatherTrackStateDims
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import random


class PairwiseRacePredictor(nn.Module):
    def __init__(
        self,
        input_dim,
        embed_dim,
        w_input_dim,
        w_embed_dim,
        ts_input_dim,
        ts_embed_dim,
        output_dim=1,
    ):
        super(PairwiseRacePredictor, self).__init__()
        self.embedding = nn.Linear(
            input_dim, embed_dim
        )  # Transform raw features into embeddings
        self.weather_embedding = nn.Embedding(
            w_input_dim, w_embed_dim, padding_idx=0
        )  # weather embedding
        self.track_state_embedding = nn.Embedding(
            ts_input_dim, ts_embed_dim, padding_idx=0
        )  # track_state_embedding
        self.fc = nn.Linear(
            embed_dim + w_embed_dim + ts_embed_dim, output_dim
        )  # Predict probabilities
        self.softmax = nn.Softmax(dim=1)  # Ensure probabilities sum to 1

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
        x_num_emb = self.embedding(x_num)

        # Embed categorical variables
        x_weather_emb = self.weather_embedding(
            x_weather
        )
        x_track_state_emb = self.track_state_embedding(
            x_track_state
        )

        # Concatenate all features
        x = torch.cat(
            [x_num_emb, x_weather_emb, x_track_state_emb], dim=-1
        )

        #print(x)
        #exit()

        #print(attn_output)
        #exit()

        # Predict probabilities
        logits = self.fc(x)

        #print(logits)
        #exit()

        probabilities = self.softmax(logits)

        #print(probabilities)
        #exit()

        return probabilities  # (batch_size, 2, 1)
    

def buildBatches(batch_size, alt):
    batches = []

    engine = local_connect("StableEarnings")

    if alt:
        query = """
            SELECT *
            FROM alttrainables
            WHERE won IS NOT NULL
            ORDER BY race_id;
        """
    else:
        query = """
            SELECT *
            FROM trainables
            ORDER BY race_id;
        """

    df = pd.read_sql_query(query, engine)

    engine.dispose()

    grouped = df.groupby("race_id")

    group_numpys = [data_frame.to_numpy() for race_id, data_frame in grouped]

    temp = []

    for arr in group_numpys:
        arr_len = len(arr)
        if arr_len < 4:
            continue
        for i in range(arr_len - 1):
            for j in range(i + 1, arr_len):
                if random.choice([True, False]) is True:
                    temp.append(
                        np.array(
                            [np.append(
                                np.append(
                                    arr[i:i+1], np.array([[1.0]]), axis=1
                                ),
                                np.append(
                                    arr[j:j+1], np.array([[0.0]]), axis=1
                                ), axis=0
                            )]
                        )
                    )
                else:
                    temp.append(
                        np.array(
                            [np.append(
                                np.append(
                                    arr[j:j+1], np.array([[0.0]]), axis=1
                                ),
                                np.append(
                                    arr[i:i+1], np.array([[1.0]]), axis=1
                                ), axis=0
                            )]
                        )
                    )

    random.shuffle(temp)

    for i in range(len(temp)):
        if i % batch_size == 0:
            batches.append(
                torch.from_numpy(
                    temp[i][:, :, 4:].astype(np.float32)
                ).float()
            )
        else:
            batches[-1] = torch.cat(
                (
                    batches[-1],
                    torch.from_numpy(
                        temp[i][:, :, 4:].astype(np.float32)
                    ).float()
                ),
                dim=0
            )

    return batches


# Define a training function
def train_model(model, batches, alt, num_epochs=10, learning_rate=0.001):
    """
    Train the RacePredictor model.

    Args:
        model (nn.Module): The RacePredictor model to train.
        batches (list): List of training batches, where each batch is a list of DataFrames.
        num_epochs (int): Number of epochs to train the model.
        learning_rate (float): Learning rate for the optimizer.
    """
    # Define loss function and optimizer
    criterion = criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    # Iterate over epochs
    for epoch in range(num_epochs):
        model.train()  # Set model to training mode
        epoch_loss = 0.0

        for batch in batches:
            # Prepare the data
            features = torch.cat(
                (batch[:, :, :41], batch[:, :, 42:43], batch[:, :, 44:-4]), dim=2
            )
            missing_mask = torch.isnan(
                features
            ).float()  # 1 for NaN, 0 for valid values
            data_filled = torch.nan_to_num(
                features, nan=0.0
            )  # fills NaNs with a placeholder
            data_with_mask = torch.cat(
                [data_filled, missing_mask], dim=-1
            )  # concatenates data with mask
            weather = batch[:, :, 41].long()  # Weather as categorical
            track_state = batch[:, :, 43].long()  # Weather as categorical

            if alt:
                targets = torch.softmax(batch[:, :, -3:-2], 1)  # pos_factor label
            else:
                targets = torch.softmax(batch[:, :, -1:], 1)  # 1s and 0s

            # Zero the gradients
            optimizer.zero_grad()

            # Forward pass
            outputs = model(data_with_mask, weather, track_state)

            loss = criterion(outputs, targets)

            # Backward pass and optimization
            loss.backward()
            optimizer.step()

            # Accumulate loss for reporting
            epoch_loss += loss.item()

        # Print epoch loss
        print(f"Epoch {epoch + 1}/{num_epochs}, Loss: {epoch_loss / len(batches)}")


if __name__ == "__main__":
    alt = False  # Use AltTrainables table with pos_factors instead of 0s and 1s
    
    # Build batches
    batches = buildBatches(32, alt)

    w_input_dim, ts_input_dim = getWeatherTrackStateDims()

    # Initialize the model
    input_dim = (
        170  # Number of input features ((87 features - 2 categoricals) * 2 for masking)
    )
    embed_dim = 54  # Embedding dimension
    w_embed_dim = 4  # weather embedding dimension
    ts_embed_dim = 6  # track_state embedding dimension
    model = PairwiseRacePredictor(
        input_dim,
        embed_dim,
        w_input_dim,
        w_embed_dim,
        ts_input_dim,
        ts_embed_dim
    )

    # Train the model
    train_model(model, batches, alt, num_epochs=20, learning_rate=0.001)

    torch.save(model.state_dict(), "race_pairwise_predictor_model.pth")
    print("Model saved as race_pairwise_predictor_model.pth")
