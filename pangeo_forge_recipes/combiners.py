import operator
from dataclasses import dataclass
from functools import reduce
from typing import Sequence, Tuple

import apache_beam as beam

from .aggregation import XarrayCombineAccumulator
from .patterns import CombineOp, DimKey, Index


@dataclass
class CombineXarraySchemas(beam.CombineFn):
    """A beam ``CombineFn`` which we can use to combine multiple xarray schemas
    along a single dimension

    :param dim_key: The dimension along which to combine
    """

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
