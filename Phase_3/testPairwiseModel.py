from sqlalchemy import create_engine
import pandas as pd
import torch
import numpy as np
import torch.nn.functional as F
import random
import seaborn as sns
import matplotlib.pyplot as plt
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

    balance = 0
    min_money = 0
    max_money = 0
    count = 0
    bets_won = 0
    random_balance = 0
    total = 0

    win_data = []
    loss_data = []

    for arr in group_numpys:
        arr_len = len(arr)
        odds = {}
        scores = {}
        for i in range(arr_len):
            odds[arr[i][1]] = arr[i][-1]
            scores[arr[i][1]] = 1
        for i in range(arr_len - 1):
            for j in range(i + 1, arr_len):
                bool = random.choice([True, False])
                if bool:
                    testable = torch.from_numpy(
                        np.array(
                            [np.append(
                                arr[i:i+1][:, 4:-1],
                                arr[j:j+1][:, 4:-1],
                                axis=0
                            )]
                        ).astype(np.float32)
                    ).float()
                else:
                    testable = torch.from_numpy(
                        np.array(
                            [np.append(
                                arr[j:j+1][:, 4:-1],
                                arr[i:i+1][:, 4:-1],
                                axis=0
                            )]
                        ).astype(np.float32)
                    ).float()

                features = torch.cat(
                    (testable[:, :, :41], testable[:, :, 42:43], testable[:, :, 44:]), dim=2
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
                weather = testable[:, :2, 41].long()  # Weather as categorical
                track_state = testable[:, :2, 43].long()  # Weather as categorical

                outputs = model(data_with_mask, weather, track_state)
                sm_outputs = F.softmax(outputs, dim=1)

                if bool:
                    scores[arr[i][1]] += sm_outputs[0][0][0].item()
                    scores[arr[j][1]] += sm_outputs[0][1][0].item()
                else:
                    scores[arr[j][1]] += sm_outputs[0][0][0].item()
                    scores[arr[i][1]] += sm_outputs[0][1][0].item()

        scores_total = np.sum(np.array(list(scores.values())))
        finish = max(scores, key=scores.get)
        score = scores[finish] / scores_total

        if score > (1 / odds[finish]):
            balance -= 1
            count += 1
            if finish == 1:
                balance += 1 + odds[finish]
                bets_won += 1
                win_data.append(odds[finish])
            else:
                loss_data.append(odds[finish])

        random_balance -= 1
        total += 1
        if random.choice(list(range(len(scores)))) == 0:
            random_balance += 1 + odds[1]

        if balance < min_money:
            min_money = balance
        if balance > max_money:
            max_money = balance

    # Create a KDE plot for 'True' and 'False' outcomes
    plt.figure(figsize=(10, 6))
    combined_data = pd.DataFrame({
        'Odds': win_data + loss_data,
        'Outcome': ['Won'] * len(win_data) + ['Lost'] * len(loss_data)
    })
    sns.kdeplot(
        data=combined_data,
        x='Odds',
        hue='Outcome',
        fill=True,
        common_norm=True
    )

    # Overlay the y = 1/x curve
    x_vals = np.linspace(1, 50, 500)  # Avoid x = 0 to prevent division by zero
    y_vals = 1 / x_vals
    plt.plot(x_vals, y_vals, label='y = 1/x', color='black', linestyle='--', linewidth=2)

    # Limit x axis
    plt.xlim(0, 50)

    # Add labels and title
    plt.title('Density Plot of Odds Values by Outcome', fontsize=16)
    plt.xlabel('Odds', fontsize=14)
    plt.ylabel('Density', fontsize=14)
    plt.legend(title='Outcome', fontsize=12)

    # Show the plot
    plt.show()

    return balance, bets_won, min_money, max_money, count, balance / count, random_balance, random_balance / total


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
    #model.load_state_dict(torch.load("race_pairwise_predictor_model.pth"))
    model.load_state_dict(torch.load("alt_race_pairwise_predictor_model.pth"))
    print(testModel(model))
