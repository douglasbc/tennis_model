import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

import get_data

# Load and preprocess the data
data = get_data.get_elevation_clustering_data()
data = data.set_index('elevation_id')

# Normalize the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(data)

# Perform K-means clustering
n_clusters = 10
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
cluster_labels = kmeans.fit_predict(normalized_data)

# Add cluster labels to the original dataset
data['Cluster'] = cluster_labels

# Print cluster members
for i in range(n_clusters):
    elevations_list = data[data['Cluster'] == i].index.tolist()
    print(f"\nCluster {i} - {len(elevations_list)} data points:")
    # print(elevations_list)

# Print cluster centroids
centroids = scaler.inverse_transform(kmeans.cluster_centers_)
centroid_df = pd.DataFrame(centroids, columns=data.columns[:-1])
print("\nCluster Centroids:")
print(centroid_df)