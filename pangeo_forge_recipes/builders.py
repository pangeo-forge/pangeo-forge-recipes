
@dataclass
class XarrayZarrRecipe(beam.PTransform):

    file_pattern: FilePattern
    target_chunks: Dict[str, int] = field(default_factory=dict)
    cache_inputs: Optional[bool] = None
    copy_input_to_local_file: bool = False
    consolidate_zarr: bool = True
    consolidate_dimension_coordinates: bool = True
    xarray_open_kwargs: dict = field(default_factory=dict)
    delete_input_encoding: bool = True
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]] = None
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]] = None
    open_input_with_kerchunk: bool = False

    def expand(self, pcoll):
        input_urls = beam.create(file_pattern)
        open_files = input_urls | OpenURLWithFSSpec(
            secrets=self.file_pattern.query_string_secrets,
            open_kwargs=self.file_pattern.fsspec_open_kwargs
        )
        dsets = open_files | OpenWithXarray(
            file_type=self.file_pattern.file_type,
            copy_to_local=self.copy_input_to_local_file,
            xarray_open_kwargs=self.xarray_open_kwargs
        )
        schemas = dsets | DatasetSchema()
    
