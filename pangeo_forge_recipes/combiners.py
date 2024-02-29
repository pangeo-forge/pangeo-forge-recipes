import operator
import sys
from dataclasses import dataclass
from functools import reduce
from typing import Callable, Sequence, TypeVar

import apache_beam as beam

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


Element = TypeVar("Element")
Accumulator = TypeVar("Accumulator")


def build_reduce_fn(
    accumulate_op: Callable[[Element, Element], Accumulator],
    merge_op: Callable[[Accumulator, Accumulator], Accumulator],
    initializer: Accumulator,
) -> beam.CombineFn:
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


# Find minimum/maximum/count values.
# The count is done as a slight optimization to avoid multiple passes across the distribution.
# Note: MyPy struggles with type inference here due to the high degree of genericity.

MinMaxCountCombineFn = build_reduce_fn(
    accumulate_op=lambda acc, input: (
        min(acc[0], input),  # type: ignore
        max(acc[1], input),  # type: ignore
        acc[2] + 1,  # type: ignore
    ),
    merge_op=lambda accLeft, accRight: (
        min(accLeft[0], accRight[0]),  # type: ignore
        max(accLeft[1], accRight[1]),  # type: ignore
        accLeft[2] + accRight[2],  # type: ignore
    ),
    initializer=(sys.maxsize, -sys.maxsize, 0),
)
