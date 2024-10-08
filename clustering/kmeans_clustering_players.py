import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

import utils


client = utils.bigquery_client()

# Load and preprocess the data
data = utils.get_player_clustering_data('atp')
# data = utils.get_player_clustering_data('wta')
data = data.set_index('player_name')

# Convert percentage strings to floats
for col in data.columns:
    if data[col].dtype == 'object':
        data[col] = data[col].str.replace(',', '.').astype(float)

# Normalize the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(data)

# Perform K-means clustering
n_clusters = 6
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
cluster_labels = kmeans.fit_predict(normalized_data)

# Add cluster labels to the original dataset
data['cluster'] = cluster_labels

# # Visualize the clusters (using the first two principal components for simplicity)
# pca = PCA(n_components=2)
# pca_result = pca.fit_transform(normalized_data)

# plt.figure(figsize=(10, 8))
# scatter = plt.scatter(pca_result[:, 0], pca_result[:, 1], c=cluster_labels, cmap='viridis')
# plt.colorbar(scatter)
# plt.title('Tennis Player Clusters')
# plt.xlabel('Unreturned Percentage')
# plt.ylabel('Rally Aggression Score')
# plt.tight_layout()
# plt.show()

# Print cluster members
for i in range(n_clusters):
    players_list = data[data['cluster'] == i].index.tolist()
    print(f"\nCluster {i} - {len(players_list)} players:")
    print(players_list)

# Print cluster centroids
centroids = scaler.inverse_transform(kmeans.cluster_centers_)
centroid_df = pd.DataFrame(centroids, columns=data.columns[:-1])
print("\nCluster Centroids:")
print(centroid_df)

centroid_df.columns = ['centroid_unreturned_pct', 'centroid_rally_agression_score']

data = data.reset_index()
data=pd.merge(data,centroid_df, left_on='cluster', right_on=centroid_df.index, how='left')

utils.load_clusters_to_bq(client, data, 'tennis-358702.raw_layer.atp_players_clusters')
# utils.load_clusters_to_bq(client, data, 'tennis-358702.raw_layer.wta_players_clusters')