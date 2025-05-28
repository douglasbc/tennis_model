import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import utils

client = utils.bigquery_client()

# Load and preprocess data
data = utils.get_serve_dependency_clustering_data('atp')
data = data.set_index('player_name')

# Convert to numeric (handled in query but double-check)
data['serve_dependency_score'] = pd.to_numeric(data['serve_dependency_score'], errors='coerce')

# Normalize the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(data)

# Perform K-means clustering
n_clusters = 5
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
cluster_labels = kmeans.fit_predict(normalized_data)

# Get centroids in original scale and sort them
centroids_original = scaler.inverse_transform(kmeans.cluster_centers_)
sorted_order = np.argsort(centroids_original.flatten())
sorted_centroids = centroids_original[sorted_order]

# Create label mapping from old to new ordered labels (1-5)
label_mapping = {old: new+1 for new, old in enumerate(sorted_order)}
reverse_mapping = {new+1: old for new, old in enumerate(sorted_order)}

# Create base results DataFrame
results = pd.DataFrame({
    'player_name': data.index,
    'original_cluster': cluster_labels
})

# Map to new ordered clusters
results['best_cluster'] = results['original_cluster'].map(label_mapping)

# Calculate distances to all centroids
distances = kmeans.transform(normalized_data)

# Get second best clusters using original clusters
sorted_dist_indices = np.argsort(distances, axis=1)
results['second_best_cluster'] = [label_mapping[idx] for idx in sorted_dist_indices[:, 1]]

# Add distances
results['distance_to_best_cluster'] = distances[np.arange(len(distances)), results['original_cluster']]
results['distance_to_second_best_cluster'] = distances[np.arange(len(distances)), sorted_dist_indices[:, 1]]

# Final DataFrame with required columns
final_df = results[['player_name', 'best_cluster', 'distance_to_best_cluster',
                    'second_best_cluster', 'distance_to_second_best_cluster']]

# Create sorted centroid DataFrame
centroid_df = pd.DataFrame(sorted_centroids,
                          columns=['centroid_serve_dependency_score'],
                          index=pd.Index([1, 2, 3, 4, 5], name='cluster'))

# Print cluster info with counts
cluster_counts = results['best_cluster'].value_counts().sort_index()
print("\nCluster Centroids (ordered from 1=lowest to 5=highest):")
for cluster, row in centroid_df.iterrows():
    print(f"Cluster {cluster} ({cluster_counts.get(cluster, 0)} players): {row[0]:.4f}")

# Upload to BigQuery
utils.load_clusters_to_bq(client, final_df, 'tennis-358702.raw_layer.atp_serve_dependency_clusters')