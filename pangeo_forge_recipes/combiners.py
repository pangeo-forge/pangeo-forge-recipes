import operator
from dataclasses import dataclass
from functools import reduce
from typing import Sequence, Tuple, TypeVar

import apache_beam as beam

from pangeo_forge_recipes.aggregation import XarrayCombineAccumulator
from pangeo_forge_recipes.patterns import CombineOp, DimKey, Index

T = TypeVar("T")
Indexed = Tuple[Index, T]


def _nest_dim(item: Indexed[T], dim_key: DimKey) -> Indexed[Indexed[T]]:
    index, value = item
    inner_index = Index({dim_key: index[dim_key]})
    outer_index = Index({dk: index[dk] for dk in index if dk != dim_key})
    return outer_index, (inner_index, value)


@dataclass
class NestDim(beam.PTransform):
    """Prepare a collection for grouping by transforming an Index into a nested
    Tuple of indexes."""

    dim_key: DimKey

    def expand(self, pcoll):
        return pcoll | beam.Map(_nest_dim, dim_key=self.dim_key)


@dataclass
class CombineNested(beam.PTransform):
    combine_dims: Sequence[DimKey]

    def expand(self, pcoll):
        cdims = self.combine_dims.copy()
        while len(cdims) > 0:
            last_dim = cdims.pop()
            if len(cdims) == 0:
                # at this point, we should have a 1D index as our key
                pcoll = pcoll | beam.CombineGlobally(CombineXarraySchemas(last_dim))
            else:
                pcoll = (
                    pcoll | NestDim(last_dim) | beam.CombinePerKey(CombineXarraySchemas(last_dim))
                )
        return pcoll


@dataclass
class CombineXarraySchemas(beam.CombineFn):
    dim_key: DimKey

    def get_position(self, index: Index) -> int:
        return index[self.dim_key].position

    def create_accumulator(self) -> XarrayCombineAccumulator:
        concat_dim = self.dim_key.name if self.dim_key.operation == CombineOp.CONCAT else None
        return XarrayCombineAccumulator(concat_dim=concat_dim)

    def add_input(self, accumulator: XarrayCombineAccumulator, item: Tuple[Index, dict]):
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
