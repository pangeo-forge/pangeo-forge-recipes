# mypy: disable-error-code="name-defined"

c.Bake.bakery_class = "pangeo_forge_runner.bakery.local.LocalDirectBakery"

c.TargetStorage.fsspec_class = "fsspec.implementations.local.LocalFileSystem"
c.TargetStorage.root_path = "./target"

c.InputCacheStorage.fsspec_class = "fsspec.implementations.local.LocalFileSystem"
c.InputCacheStorage.root_path = "./cache"
