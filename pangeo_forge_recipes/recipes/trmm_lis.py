"""
Script converts a given netcdf file to COG (Cloud Optimized Geotiff Image)
"""
import os

import s3fs
import xarray as xa


class TrmmLisRecipe:
    def __init__(self, patterns, store_directory, var):
        """TrmmLisRecipe Constructor. Convert netcdf files to COG (Cloud Optimized Geotiff Images)
        :param patterns: pattern object which contains file paths, s3 paths are preferred
        :param store_directory: Path to where the generated COG should be stored
        :param var: Variable inside the nc files which will be used to convert to COG
        """
        self.paths = []
        self.directory = store_directory
        self.var = var
        self.pattern = patterns
        self.file_pattern_to_paths()
        self.generate_cog()

    def file_pattern_to_paths(self):
        """
        Gets the paths from file patterns and stores it in list
        """
        for index, fname in self.pattern.items():
            self.paths.append(fname[0])

    def generate_cog(self):
        """
        Main piece of code that converts given TRMM-LIS netcdf files to COG
        """
        fs = s3fs.S3FileSystem(anon=False)
        for path in self.paths:

            with fs.open(path) as file:
                print("Starting File: " + path + " ....", end="")
                file = xa.open_dataset(file)  # open nc datasets
                flash_rate_ds = file[self.var]  # get flash_rate based on given variable

                if self.var == "VHRFC_LIS_FRD":
                    grid = flash_rate_ds[::-1]  # Orientation is flipped to the correct position
                    rows = grid.to_numpy()[:, :] == 0.0
                    grid.to_numpy()[rows] = None

                    grid.rio.set_spatial_dims(x_dim="Longitude", y_dim="Latitude", inplace=True)
                    grid.rio.crs
                    grid.rio.set_crs("epsg:4326")

                    cog_name = f"{self.var}_co.tif"
                    cog_path = f"{self.directory}/{cog_name}"
                    grid.rio.to_raster(rf"{cog_path}", driver="COG")
                else:
                    ds_type = flash_rate_ds.dims[0]
                    for grid in flash_rate_ds:
                        grid = grid[::-1]  # Orientation is flipped to the correct position
                        rows = grid.to_numpy()[:, :] == 0.0
                        grid.to_numpy()[rows] = None
                        grid_index = grid.coords[ds_type].values

                        grid.rio.set_spatial_dims(x_dim="Longitude", y_dim="Latitude", inplace=True)
                        grid.rio.crs
                        grid.rio.set_crs("epsg:4326")

                        cog_name = f"{self.var}_{ds_type}_{grid_index}_co.tif"
                        cog_path = cog_path = f"{self.directory}/{cog_name}"
                        grid.rio.to_raster(rf"{cog_path}", driver="COG")
            print("Complete!!")
