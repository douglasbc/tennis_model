from sklearn.preprocessing import StandardScaler
from sklearn.mixture import BayesianGaussianMixture

from get_data import get_player_clustering_data

# Load and preprocess the data
data = get_clustering_data('atp')
# data = get_clustering_data('wta')
data = data.set_index('player_name')

# Convert percentage strings to floats
for col in data.columns:
    if data[col].dtype == 'object':
        data[col] = data[col].str.replace(',', '.').astype(float)

# Normalize the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(data)

# Perform Bayesian Gaussian Mixture Models clustering
bgm = BayesianGaussianMixture(n_components=10, n_init=10, random_state=42)
bgm.fit(normalized_data)
print(bgm.weights_.round(2))