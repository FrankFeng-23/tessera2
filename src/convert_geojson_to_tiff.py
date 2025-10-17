import json
import numpy as np
import rasterio
from rasterio import features
from rasterio.transform import from_bounds
from rasterio.crs import CRS
from shapely.geometry import shape, mapping
import geopandas as gpd
from pyproj import CRS as pyCRS

def get_utm_zone(lon):
    """根据经度计算 UTM 区号"""
    return int((lon + 180) / 6) + 1

def get_best_projection(bounds):
    """根据数据边界选择最佳投影坐标系"""
    # 计算中心经纬度
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    
    # 对于英国剑桥地区，使用英国国家格网 (British National Grid)
    # EPSG:27700 是专门为英国设计的投影系统
    if -8 < center_lon < 2 and 49 < center_lat < 61:
        return CRS.from_epsg(27700)
    
    # 否则使用 UTM
    utm_zone = get_utm_zone(center_lon)
    hemisphere = 'north' if center_lat >= 0 else 'south'
    epsg_code = 32600 + utm_zone if hemisphere == 'north' else 32700 + utm_zone
    return CRS.from_epsg(epsg_code)

def geojson_to_tiff(geojson_path, output_tiff_path, resolution=10):
    """
    将 GeoJSON 文件转换为 TIFF 文件
    
    参数:
    - geojson_path: 输入的 GeoJSON 文件路径
    - output_tiff_path: 输出的 TIFF 文件路径
    - resolution: 栅格分辨率（米）
    """
    
    # 读取 GeoJSON 文件
    print(f"读取 GeoJSON 文件: {geojson_path}")
    with open(geojson_path, 'r') as f:
        geojson_data = json.load(f)
    
    # 使用 geopandas 读取以获取边界
    gdf = gpd.read_file(geojson_path)
    
    # 获取数据边界（WGS84）
    bounds_wgs84 = gdf.total_bounds  # [minx, miny, maxx, maxy]
    print(f"WGS84 边界: {bounds_wgs84}")
    
    # 选择最佳投影坐标系
    target_crs = get_best_projection(bounds_wgs84)
    print(f"使用投影坐标系: {target_crs}")
    
    # 投影 GeoDataFrame
    gdf_projected = gdf.to_crs(target_crs)
    bounds = gdf_projected.total_bounds
    print(f"投影后边界: {bounds}")
    
    # 计算栅格尺寸
    width = int((bounds[2] - bounds[0]) / resolution) + 1
    height = int((bounds[3] - bounds[1]) / resolution) + 1
    print(f"栅格尺寸: {width} x {height} 像素")
    
    # 创建仿射变换
    transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], width, height)
    
    # 准备几何形状列表（投影后的）
    shapes = []
    for feature in geojson_data['features']:
        geom = shape(feature['geometry'])
        # 创建临时 GeoDataFrame 进行投影
        temp_gdf = gpd.GeoDataFrame([1], geometry=[geom], crs='EPSG:4326')
        temp_gdf_proj = temp_gdf.to_crs(target_crs)
        shapes.append((mapping(temp_gdf_proj.geometry[0]), 1))
    
    # 创建空栅格
    raster = np.zeros((height, width), dtype=np.uint8)
    
    # 栅格化
    print("正在栅格化...")
    features.rasterize(
        shapes=shapes,
        out=raster,
        transform=transform,
        fill=0,
        default_value=1,
        dtype=np.uint8
    )
    
    # 写入 TIFF 文件（使用压缩以减小文件大小）
    print(f"写入 TIFF 文件: {output_tiff_path}")
    with rasterio.open(
        output_tiff_path,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype=np.uint8,
        crs=target_crs,
        transform=transform,
        compress='deflate',  # 使用 DEFLATE 压缩
        predictor=2,  # 使用预测器以提高压缩率
        tiled=True,  # 使用瓦片存储
        blockxsize=256,  # 瓦片大小
        blockysize=256
    ) as dst:
        dst.write(raster, 1)
    
    # 输出文件信息
    print(f"\n转换完成！")
    print(f"输出文件: {output_tiff_path}")
    print(f"栅格尺寸: {width} x {height} 像素")
    print(f"像素分辨率: {resolution} 米")
    print(f"投影坐标系: {target_crs}")
    
    # 计算并显示文件大小
    import os
    file_size = os.path.getsize(output_tiff_path) / (1024 * 1024)
    print(f"文件大小: {file_size:.2f} MB")

# 使用示例
if __name__ == "__main__":
    # 输入和输出路径
    geojson_path = "/scratch/zf281/tessera/data/cambridge/geoinfo/CB.geojson"
    output_tiff_path = "/scratch/zf281/tessera/data/cambridge/geoinfo/CB.tiff"
    
    # 执行转换
    geojson_to_tiff(geojson_path, output_tiff_path, resolution=10)