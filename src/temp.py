import numpy as np
import matplotlib.pyplot as plt

band_file_path  = "/scratch/zf281/tessera2/data/borneo/output/2024/data_processed/landsat_bands.npy"
band_data = np.load(band_file_path, mmap_mode='r')
print(band_data.shape)

first_time_slice = band_data[0, ...]
# to f32
first_time_slice = first_time_slice.astype(np.float32)
rgb = first_time_slice[:,:, :3]  # Assuming the first three bands are RGB
# 归一化
for i in range(3):
    rgb[:,:,i] = (rgb[:,:,i] - np.min(rgb[:,:,i])) / (np.max(rgb[:,:,i]) - np.min(rgb[:,:,i]))
plt.imshow(rgb)
plt.imsave('rgb_image.png', rgb)

mask_file_path = "/scratch/zf281/tessera2/data/borneo/output/2024/data_processed/landsat_masks.npy"
mask_data = np.load(mask_file_path, mmap_mode='r')
print(mask_data.shape)
first_time_slice = mask_data[0, ...]
plt.imshow(first_time_slice, cmap='gray')
plt.imsave('mask_image.png', first_time_slice, cmap='gray')