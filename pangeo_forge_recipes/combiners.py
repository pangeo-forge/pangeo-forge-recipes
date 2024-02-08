import operator
from dataclasses import dataclass, field
from functools import reduce
from typing import Dict, List, Optional, Sequence

import apache_beam as beam
import fsspec
from kerchunk.combine import MultiZarrToZarr

from .aggregation import XarrayCombineAccumulator, XarraySchema
from .types import CombineOp, Dimension, Index, Indexed


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

    def add_input(self, accumulator: XarrayCombineAccumulator, item: Indexed[XarraySchema]):
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
class CombineZarrRefs(beam.CombineFn):
    """A beam ``CombineFn`` for combining Kerchunk reference objects.

    :param concat_dims: Dimensions along which to concatenate inputs.
    :param identical_dims: Dimensions shared among all inputs.
    :param remote_options: Storage options for opening remote files
    :param remote_protocol: If files are accessed over the network, provide the remote protocol
      over which they are accessed. e.g.: "s3", "gcp", "https", etc.
    :mzz_kwargs: Additional kwargs to pass to ``kerchunk.combine.MultiZarrToZarr``.
    :param target_options: Target options dict to pass to the MultiZarrToZarr

    """

    concat_dims: List[str]
    identical_dims: List[str]
    target_options: Optional[Dict] = field(default_factory=lambda: {"anon": True})
    remote_options: Optional[Dict] = field(default_factory=lambda: {"anon": True})
    remote_protocol: Optional[str] = None
    mzz_kwargs: dict = field(default_factory=dict)

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

    def create_accumulator(self) -> list[dict]:
        return []

    def add_input(self, accumulator: list[dict], item: dict) -> list[dict]:
        accumulator.append(item)
        return accumulator

    def merge_accumulators(self, accumulators: list[list[dict]]) -> list[dict]:
        return [self.to_mzz(accumulator).translate() for accumulator in accumulators]

    def extract_output(self, accumulator: list[dict]) -> fsspec.FSMap:
        return fsspec.filesystem(
            "reference",
            fo=self.to_mzz(accumulator).translate(),
            storage_options={
                "remote_protocol": self.remote_protocol,
                "skip_instance_cache": True,
            },
        ).get_mapper()
