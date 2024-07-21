from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt

from get_data import get_clustering_data

# Load and preprocess the data
# data = get_clustering_data('atp')
data = get_clustering_data('wta')
data = data.set_index('player_name')

# Convert percentage strings to floats
for col in data.columns:
    if data[col].dtype == 'object':
        data[col] = data[col].str.replace(',', '.').astype(float)

# Normalize the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(data)

# Function to compute WCSS for Elbow Method
def compute_wcss(data):
    wcss = []
    for n in range(1, 11):
        kmeans = KMeans(n_clusters=n, random_state=42)
        kmeans.fit(data)
        wcss.append(kmeans.inertia_)
    return wcss

# Elbow Method
wcss = compute_wcss(normalized_data)

plt.figure(figsize=(10, 5))
plt.subplot(1, 2, 1)
plt.plot(range(1, 11), wcss)
plt.title('Elbow Method')
plt.xlabel('Number of clusters')
plt.ylabel('WCSS')

# Silhouette Analysis
silhouette_scores = []
for n in range(2, 11):  # Start from 2 clusters
    kmeans = KMeans(n_clusters=n, random_state=42)
    cluster_labels = kmeans.fit_predict(normalized_data)
    silhouette_avg = silhouette_score(normalized_data, cluster_labels)
    silhouette_scores.append(silhouette_avg)

plt.subplot(1, 2, 2)
plt.plot(range(2, 11), silhouette_scores)
plt.title('Silhouette Analysis')
plt.xlabel('Number of clusters')
plt.ylabel('Silhouette Score')

plt.tight_layout()
plt.show()

# Print the silhouette scores
for n, score in enumerate(silhouette_scores, 2):
    print(f"For n_clusters = {n}, the average silhouette score is : {score:.4f}")