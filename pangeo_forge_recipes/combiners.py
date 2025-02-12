import copy
import sys
from dataclasses import dataclass
from functools import reduce
from typing import Callable, Optional, Sequence, Tuple, TypeVar

import apache_beam as beam

from .aggregation import XarraySchema, _combine_xarray_schemas
from .types import CombineOp, Dimension, Index, Indexed

SchemaAccumulator = Tuple[Optional[XarraySchema], Optional[str]]


@dataclass
class CombineXarraySchemas(beam.CombineFn):
    """A beam ``CombineFn`` which we can use to combine multiple xarray schemas
    along a single dimension

    :param dimension: The dimension along which to combine
    """

    dimension: Dimension

    def get_position(self, index: Index) -> int:
        return index[self.dimension].value

    def create_accumulator(self) -> SchemaAccumulator:
        concat_dim = self.dimension.name if self.dimension.operation == CombineOp.CONCAT else None
        return (None, concat_dim)

    def add_input(self, accumulator: SchemaAccumulator, item: Indexed[XarraySchema]) -> SchemaAccumulator:
        acc_schema, acc_concat_dim = accumulator
        next_index, next_schema = item
        if acc_concat_dim:
            assert acc_concat_dim not in next_schema["chunks"], "Concat dim should be unchunked for new input"
            position = self.get_position(next_index)
            # Copy to avoid side effects (just python things)
            next_schema = copy.deepcopy(next_schema)
            next_schema["chunks"][acc_concat_dim] = {position: next_schema["dims"][acc_concat_dim]}
        if acc_schema:
            result = (
                _combine_xarray_schemas(acc_schema, next_schema, concat_dim=acc_concat_dim),
                acc_concat_dim,
            )
        else:
            result = (next_schema, acc_concat_dim)
        return result

    def merge_accumulators(self, accumulators: Sequence[SchemaAccumulator]) -> SchemaAccumulator:
        if len(set([accumulator[1] for accumulator in accumulators])) > 1:
            raise ValueError("Can't merge accumulators with different concat_dims")
        else:
            return reduce(
                lambda acc1, acc2: (_combine_xarray_schemas(acc1[0], acc2[0], acc1[1]), acc1[1]),
                accumulators,
                self.create_accumulator(),
            )

    def extract_output(self, accumulator) -> XarraySchema:
        # Toss out the concat dim info and just take the schema
        return accumulator[0]


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
