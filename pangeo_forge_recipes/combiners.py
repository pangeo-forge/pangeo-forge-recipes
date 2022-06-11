from dataclasses import dataclass
from typing import Sequence, Tuple

import apache_beam as beam

from pangeo_forge_recipes.aggregation import XarrayCombineAccumulator
from pangeo_forge_recipes.patterns import CombineOp, Index


@dataclass
class CombineXarraySchemas(beam.CombineFn):
    name: str
    operation: CombineOp

    def get_position(self, index: Index):
        possible_indexes = [
            didx
            for didx in index
            if (didx.name == self.name) and (didx.operation == self.operation)
        ]
        assert len(possible_indexes) == 1, "More than one dim detected"
        return possible_indexes[0].index

    def create_accumulator(self) -> XarrayCombineAccumulator:
        concat_dim = self.name if self.operation == CombineOp.CONCAT else None
        return XarrayCombineAccumulator(concat_dim=concat_dim)

    def add_input(self, accumulator: XarrayCombineAccumulator, item: Tuple[Index, dict]):
        index, schema = item
        position = self.get_position(index)
        accumulator.add_input(schema, position)
        return accumulator

    def merge_accumulators(self, accumulators: Sequence[XarrayCombineAccumulator]):
        if len(accumulators) == 1:
            return accumulators[0]
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator.schema
