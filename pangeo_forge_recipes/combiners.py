from typing import Sequence
from dataclasses import dataclass

import apache_beam as beam

from pangeo_forge_recipes.patterns import Index, CombineOp
from pangeo_forge_recipes.aggregation import XarrayConcatAccumulator


def _get_position(index: Index, concat_dim: str):
    possible_indexes = [
        didx for didx in index
        if didx.operation == CombineOp.CONCAT
        and didx.name == concat_dim
    ]
    assert len(possible_indexes) == 1, "More than one concat dim detected."
    return possible_indexes[0].index



@dataclass
class ConcatXarraySchemas(beam.CombineFn):
    concat_dim: str

    def create_accumulator(self) -> XarrayConcatAccumulator:
        return XarrayConcatAccumulator(self.concat_dim)

    def add_input(self, accumulator: XarrayConcatAccumulator, item: tuple[Index, dict]):
        index, schema = item
        position = _get_position(index, self.concat_dim)
        accumulator.add_input(schema, position)
        return accumulator

    def merge_accumulators(self, accumulators: Sequence[XarrayConcatAccumulator]):
        if len(accumulators) == 1:
            return accumulators[0]
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator.schema, accumulator.chunk_lens
