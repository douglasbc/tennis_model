import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

import utils


client = utils.bigquery_client()

# Load and preprocess the data
data = utils.get_elevation_clustering_data()
data = data.set_index('geo_id')

# Normalize the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(data)

# Perform K-means clustering
n_clusters = 4
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
cluster_labels = kmeans.fit_predict(normalized_data)

# Add cluster labels to the original dataset
data['cluster'] = cluster_labels

# Print cluster members
for i in range(n_clusters):
    elevations_list = data[data['cluster'] == i].index.tolist()
    # print(f"\nCluster {i} - {len(elevations_list)} data points:")
    # print(elevations_list)

# Print cluster centroids
centroids = scaler.inverse_transform(kmeans.cluster_centers_)
centroid_df = pd.DataFrame(centroids, columns=data.columns[:-1])
centroid_df.columns = ['centroid']

data=pd.merge(data,centroid_df, left_on='cluster', right_on=centroid_df.index, how='left')


utils.load_clusters_to_bq(client, data, 'tennis-358702.raw_layer.elevation_clusters')