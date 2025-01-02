from sqlalchemy import create_engine
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from buildPairwiseModel import PairwiseRacePredictor
from buildModel import getWeatherTrackStateDims, local_connect


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

    testables = []

    for arr in group_numpys:
        arr_len = len(arr)
        if arr_len < 4:
            continue
        for i in range(arr_len - 1):
            for j in range(i + 1, arr_len):
                testables.append(
                    torch.from_numpy(
                        np.array(
                            [np.append(
                                np.append(
                                    arr[i:i+1][:, 4:], np.array([[1.0]]), axis=1
                                ),
                                np.append(
                                    arr[j:j+1][:, 4:], np.array([[0.0]]), axis=1
                                ), axis=0
                            )]
                        ).astype(np.float32)
                    ).float()
                )

    weird = 0
    total = 0
    for t in testables:
        # Prepare the data
        features = torch.cat(
            (t[:, :, :41], t[:, :, 42:43], t[:, :, 44:-2]), dim=2
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
        weather = t[:, :2, 41].long()  # Weather as categorical
        track_state = t[:, :2, 43].long()  # Weather as categorical
        odds = t[:, :2, -1:]  # Labels

        outputs = model(data_with_mask, weather, track_state)

        if not ((0.999 < outputs[0][0][0].item() < 1.001 and -0.001 < outputs[0][1][0].item() < 0.001)
                or (-0.001 < outputs[0][0][0].item() < 0.001 and 0.999 < outputs[0][1][0].item() < 1.001)):
            weird += 1
        total += 1
    print(weird / total)
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
    model = PairwiseRacePredictor(
        input_dim,
        embed_dim,
        w_input_dim,
        w_embed_dim,
        ts_input_dim,
        ts_embed_dim
    )

    # Load the saved model
    model.load_state_dict(torch.load("race_pairwise_predictor_model.pth"))
    testModel(model)
