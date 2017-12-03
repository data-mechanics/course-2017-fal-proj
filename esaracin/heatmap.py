import folium
from folium import plugins
import numpy as np

np.random.seed(3141592)
initial_data = (
    np.random.normal(size=(100, 2)) * np.array([[1, 1]]) +
    np.array([[48, 5]])
)


move_data = np.random.normal(size=(100, 2)) * 0.01

print(move_data)

data = [(initial_data + move_data * i).tolist() for i in range(100)]

m = folium.Map([48., 5.], tiles='stamentoner', zoom_start=6)
hm = plugins.HeatMapWithTime(data)
hm.add_to(m)

m.save('heat.html')
