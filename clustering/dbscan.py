import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt

from get_data import get_clustering_data

# Load and preprocess the data
data = get_clustering_data('atp')
# data = get_clustering_data('wta')
data = data.set_index('player_name')

# Convert percentage strings to floats
for col in data.columns:
    if data[col].dtype == 'object':
        data[col] = data[col].str.replace(',', '.').astype(float)

# Select features for clustering
features = ['unreturned_pct', 'rally_agression_score']
X = data[features]

# Standardize the features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Perform DBSCAN clustering
dbscan = DBSCAN(eps=0.5, min_samples=5)
labels = dbscan.fit_predict(X_scaled)

# Add cluster labels to the dataframe
data['Cluster'] = labels

# Print cluster information
n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
print(f"Number of clusters: {n_clusters}")

for i in range(-1, n_clusters):
    if i == -1:
        cluster_name = "Noise"
    else:
        cluster_name = f"Cluster {i}"
    
    cluster_data = data[data['Cluster'] == i]
    print(f"\n{cluster_name}:")
    print(f"Number of players: {len(cluster_data)}")
    print("Top 5 players:")
    print(cluster_data['player_name'].head().tolist())
    if i != -1:
        print("Cluster center:")
        print(scaler.inverse_transform(X_scaled[labels == i].mean(axis=0)))

# Visualize the clusters (using the first two features)
plt.figure(figsize=(10, 8))
scatter = plt.scatter(X_scaled[:, 0], X_scaled[:, 1], c=labels, cmap='viridis')
plt.title('DBSCAN Clustering of Tennis Players')
plt.xlabel('Standardized Unreturned Percentage')
plt.ylabel('Standardized Rally Aggression Score')
plt.colorbar(scatter)
plt.show()

# # Save results to a CSV file
# data.to_csv('tennis_data_clustered_dbscan.csv', index=False)