from sklearn.preprocessing import StandardScaler
from sklearn.mixture import BayesianGaussianMixture

import clustering.utils as utils

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

# Perform Bayesian Gaussian Mixture Models clustering
bgm = BayesianGaussianMixture(n_components=10, n_init=10, random_state=42)
bgm.fit(normalized_data)
print(bgm.weights_.round(2))