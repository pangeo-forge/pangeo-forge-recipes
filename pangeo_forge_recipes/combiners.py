import operator
from dataclasses import dataclass, field
from functools import reduce
from typing import Dict, List, Optional, Sequence, Tuple

import apache_beam as beam
import fsspec
from kerchunk.combine import MultiZarrToZarr

from .aggregation import XarrayCombineAccumulator, XarraySchema
from .types import CombineOp, Dimension, Index


@dataclass
class CombineXarraySchemas(beam.CombineFn):
    """A beam ``CombineFn`` which we can use to combine multiple xarray schemas
    along a single dimension

    :param dimension: The dimension along which to combine
    """

    dimension: Dimension

    def get_position(self, index: Index) -> int:
        return index[self.dimension].value

    def create_accumulator(self) -> XarrayCombineAccumulator:
        concat_dim = self.dimension.name if self.dimension.operation == CombineOp.CONCAT else None
        return XarrayCombineAccumulator(concat_dim=concat_dim)

    def add_input(self, accumulator: XarrayCombineAccumulator, item: Tuple[Index, XarraySchema]):
        index, schema = item
        position = self.get_position(index)
        accumulator.add_input(schema, position)
        return accumulator

    def merge_accumulators(
        self, accumulators: Sequence[XarrayCombineAccumulator]
    ) -> XarrayCombineAccumulator:
        if len(accumulators) == 1:
            return accumulators[0]
        # mypy did not like sum(accumulators)
        return reduce(operator.add, accumulators)

    def extract_output(self, accumulator) -> dict:
        return accumulator.schema


@dataclass
class CombineMultiZarrToZarr(beam.CombineFn):
    """A beam ``CombineFn`` for combining Kerchunk ``MultiZarrToZarr`` objects.

    :param concat_dims: Dimensions along which to concatenate inputs.
    :param identical_dims: Dimensions shared among all inputs.
    :param remote_options: Storage options for opening remote files
    :param remote_protocol: If files are accessed over the network, provide the remote protocol
      over which they are accessed. e.g.: "s3", "gcp", "https", etc.
    :mzz_kwargs: Additional kwargs to pass to ``kerchunk.combine.MultiZarrToZarr``.
    :precombine_inputs: If ``True``, precombine each input with itself, using
      ``kerchunk.combine.MultiZarrToZarr``, before adding it to the accumulator.
      Used for multi-message GRIB2 inputs, which produce > 1 reference when opened
      with kerchunk's ``scan_grib`` function, and therefore need to be consolidated
      into a single reference before adding to the accumulator. Also used for inputs
      consisting of single reference, for cases where the output dataset concatenates
      along a dimension that does not exist in the individual inputs. In this latter
      case, precombining adds the additional dimension to the input so that its
      dimensionality will match that of the accumulator.
    :param target_options: Target options dict to pass to the MultiZarrToZarr

    """

    concat_dims: List[str]
    identical_dims: List[str]
    target_options: Optional[Dict] = field(default_factory=lambda: {"anon": True})
    remote_options: Optional[Dict] = field(default_factory=lambda: {"anon": True})
    remote_protocol: Optional[str] = None
    mzz_kwargs: dict = field(default_factory=dict)
    precombine_inputs: bool = False

    def to_mzz(self, references):
        return MultiZarrToZarr(
            references,
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            target_options=self.target_options,
            remote_options=self.remote_options,
            remote_protocol=self.remote_protocol,
            **self.mzz_kwargs,
        )
    
    def dump_dicts_to_file(dicts, base_filename='translated_mzz'):
        import os
        import json
        # Get the current process ID
        pid = os.getpid()

        # Construct the filename with the PID
        filename = f"/tmp/{base_filename}_{pid}.json"

        # Open the file and dump the dictionaries
        with open(filename, 'w') as f:
            json.dump(dicts, f, indent=4)

        print(f"Data dumped to {filename} by PID {pid}")

    def create_accumulator(self):
        return None

    def add_input(self, accumulator: MultiZarrToZarr, item: list[dict]) -> MultiZarrToZarr:
        item = item if not self.precombine_inputs else [self.to_mzz(item).translate()]
        if not accumulator:
            references = item
        else:
            references = [accumulator.translate()] + item
        return self.to_mzz(references)

    def merge_accumulators(self, accumulators: Sequence[MultiZarrToZarr]) -> MultiZarrToZarr:
        references = [a.translate() for a in accumulators]
        self.dump_dicts_to_file(references, "merge_accumulators")
        return self.to_mzz(references)

    def extract_output(self, accumulator: MultiZarrToZarr) -> MultiZarrToZarr:
        self.dump_dicts_to_file(accumulator.translate(), "extract_output")
        return fsspec.filesystem(
            "reference",
            fo=accumulator.translate(),
            storage_options={
                "remote_protocol": self.remote_protocol,
                "skip_instance_cache": True,
            },
        ).get_mapper()
