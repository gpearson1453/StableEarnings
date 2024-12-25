import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

class RacePredictor(nn.Module):
    def __init__(self, input_dim, embed_dim, num_heads, output_dim=3):
        super(RacePredictor, self).__init__()
        self.embedding = nn.Linear(input_dim, embed_dim)  # Transform raw features into embeddings
        self.attention = nn.MultiheadAttention(embed_dim, num_heads, batch_first=True)
        self.fc = nn.Linear(embed_dim, output_dim)  # Predict 3 probabilities for each car
        self.softmax = nn.Softmax(dim=-1)  # Ensure probabilities sum to 1 for each car

    def forward(self, x):
        # x: (batch_size, num_cars, input_dim)
        
        # Embed raw features
        x = self.embedding(x)  # (batch_size, num_cars, embed_dim)
        
        # Apply multihead attention
        attn_output, _ = self.attention(x, x, x)  # Self-attention: Q = K = V = x
        
        # Predict probabilities
        logits = self.fc(attn_output)  # (batch_size, num_cars, output_dim)
        probs = self.softmax(logits)   # Convert logits to probabilities
        
        return probs  # (batch_size, num_cars, output_dim)

# Example usage
input_dim = 2   # Weight and horsepower
embed_dim = 16  # Embedding size
num_heads = 4   # Number of attention heads

model = RacePredictor(input_dim, embed_dim, num_heads)
batch_size = 3
num_cars = 10  # Example race with 10 cars

inputs = torch.rand(batch_size, num_cars, input_dim)  # Random input
outputs = model(inputs)
print("Output shape:", outputs.shape)  # (batch_size, num_cars, 3)

# Example training loop

# Define the model, loss function, and optimizer
model = RacePredictor(input_dim=2, embed_dim=16, num_heads=4)
criterion = nn.CrossEntropyLoss()  # For multi-class classification
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Dummy training data
batch_size = 5
num_cars = [4, 7, 10, 15, 20]  # Variable number of cars in each batch
epochs = 10

for epoch in range(epochs):
    for i, n_cars in enumerate(num_cars):
        inputs = torch.rand(batch_size, n_cars, 2)  # Random weight and horsepower
        targets = torch.randint(0, 3, (batch_size, n_cars))  # Random labels (0, 1, or 2)
        
        # Forward pass
        outputs = model(inputs)
        
        # Reshape outputs and targets for loss computation
        outputs = outputs.view(-1, 3)  # Flatten (batch_size * num_cars, 3)
        targets = targets.view(-1)    # Flatten (batch_size * num_cars)
        
        # Compute loss
        loss = criterion(outputs, targets)
        
        # Backpropagation and optimization
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        print(f"Epoch {epoch + 1}, Batch {i + 1}, Loss: {loss.item()}")
