import operator
import sys
from dataclasses import dataclass
from functools import reduce
from typing import Sequence, Tuple

import apache_beam as beam

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


def build_reduce_fn(accumulate_op, merge_op, initializer):
    """Factory to construct reducers without so much ceremony"""

    class AnonymousCombineFn(beam.CombineFn):
        def create_accumulator(self):
            return initializer

        def add_input(self, accumulator, input):
            return accumulate_op(accumulator, input)

        def merge_accumulators(self, accumulators):
            acc = accumulators[0]
            for accumulator in accumulators[1:]:
                acc = merge_op(acc, accumulator)
            return acc

        def extract_output(self, accumulator):
            return accumulator

    return AnonymousCombineFn


# Find minimum/maximum/count values
# Count done as a slight optimization to avoid multiple passes across the distribution
MinMaxCountCombineFn = build_reduce_fn(
    accumulate_op=lambda acc, input: (min(acc[0], input), max(acc[1], input), acc[2] + 1),
    merge_op=lambda accL, accR: (min(accL[0], accR[0]), max(accL[1], accR[1]), accL[2] + accR[2]),
    initializer=(sys.maxsize, -sys.maxsize, 0),
)
