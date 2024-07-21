import pandas as pd
import numpy as np
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

from get_data import get_clustering_data

# Load and preprocess the data
# data = get_clustering_data('atp')
data = get_clustering_data('wta')
data = data.set_index('player_name')

# Convert percentage strings to floats
# data['unreturned_pct'] = data['unreturned_pct'].str.replace(',', '.').astype(float)

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

# # Determine the optimal number of clusters using BIC
# n_components_range = range(1, 10)
# bic = []
# for n_components in n_components_range:
#     gmm = GaussianMixture(n_components=n_components, random_state=42)
#     gmm.fit(X_scaled)
#     bic.append(gmm.bic(X_scaled))

# optimal_n_components = n_components_range[np.argmin(bic)]

# Manually set number of clusters
optimal_n_components = 5

# Fit the GMM with the optimal number of components
gmm = GaussianMixture(n_components=optimal_n_components, random_state=42)
gmm.fit(X_scaled)

# Predict cluster labels
labels = gmm.predict(X_scaled)

# Add cluster labels to the dataframe
data['Cluster'] = labels

# Print cluster information
for i in range(optimal_n_components):
    cluster_data = data[data['Cluster'] == i]
    print(f"\nCluster {i}:")
    print(f"Number of players: {len(cluster_data)}")
    print("Top 5 players:")
    print(cluster_data['player_name'].head().tolist())
    print("Cluster center:")
    print(scaler.inverse_transform(gmm.means_[i]))

# Visualize the clusters (using the first two features)
plt.figure(figsize=(10, 8))
scatter = plt.scatter(X_scaled[:, 0], X_scaled[:, 1], c=labels, cmap='viridis')
plt.title('Gaussian Mixture Model Clustering of Tennis Players')
plt.xlabel('Standardized Unreturned Percentage')
plt.ylabel('Standardized Rally Aggression Score')
plt.colorbar(scatter)
plt.show()

# Save results to a CSV file
# data.to_csv('tennis_data_clustered.csv', index=False)