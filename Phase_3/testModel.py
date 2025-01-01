from sqlalchemy import create_engine
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from buildModel import RacePredictor, getWeatherTrackStateDims, local_connect


def testModel(model):
    model.eval()  # Set the model to evaluation mode

    engine = local_connect("StableEarnings")

    query = """
        SELECT *
        FROM testables
        ORDER BY race_id;
    """

    df = pd.read_sql_query(query, engine)

    engine.dispose()

    grouped = df.groupby("race_id")

    group_numpys = [data_frame.to_numpy() for race_id, data_frame in grouped]

    for item in group_numpys:
        item[:, -1] = item[:, -1] / np.sum(item[:, -1])

    testables = [torch.from_numpy(item[:, 4:].astype(np.float32)).float().unsqueeze(0) for item in group_numpys]

    for t in testables:
        # Prepare the data
        features = torch.cat(
            (t[:, :, :41], t[:, :, 42:43], t[:, :, 44:-1]), dim=2
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
        weather = t[:, :, 41].long()  # Weather as categorical
        track_state = t[:, :, 43].long()  # Weather as categorical
        odds = t[:, :, -1:]  # Labels

        outputs = model(data_with_mask, weather, track_state)

        #print(t)
        print(odds)
        print(outputs)
        exit()

    return


if __name__ == "__main__":
    w_input_dim, ts_input_dim = getWeatherTrackStateDims()

    # Initialize the model
    input_dim = (
        170  # Number of input features ((87 features - 2 categoricals) * 2 for masking)
    )
    embed_dim = 54  # Embedding dimension
    w_embed_dim = 4  # weather embedding dimension
    ts_embed_dim = 6  # track_state embedding dimension
    num_heads = 8  # Number of attention heads
    model = RacePredictor(
        input_dim,
        embed_dim,
        w_input_dim,
        w_embed_dim,
        ts_input_dim,
        ts_embed_dim,
        num_heads,
    )

    # Load the saved model
    model.load_state_dict(torch.load("race_predictor.pth"))
    testModel(model)
