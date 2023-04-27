import operator
from dataclasses import dataclass, field
from functools import reduce
from typing import List, Sequence, Tuple

import apache_beam as beam
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
    """A beam ``CombineFn`` for combining Kerchunk ``MultiZarrToZarr`` objects."""

    concat_dims: List[str]
    identical_dims: List[str]
    mzz_kwargs: dict = field(default_factory=dict)
    eager_combine: bool = False

    def to_mzz(self, references):
        return MultiZarrToZarr(
            references,
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            **self.mzz_kwargs,
        )

    def translate(self, mzz: MultiZarrToZarr) -> dict:
        """Wrapper for ``MultiZarrToZarr.translate`` that captures the commonly seen chunk size
        mismatch error, and provides a more descriptive error message to the user.
        """
        try:
            return mzz.translate()
        except ValueError as e:
            if "Found chunk size mismatch" not in str(e):
                raise e
            else:
                # TODO: this first hint is possibly generic enough to upstream to kerchunk
                hints = (
                    f"Kerchunk hit chunk size mismatch error:\n{str(e)}\n"
                    "Sometimes, this occurs when an identical dim is omitted. "
                    f"Are any shared dimensions missing from {self.identical_dims = }? "
                )
                # the next hint is specific to this beam.CombineFn so likely cannot be upstreamed
                hints += (
                    (
                        "If identical dims are correct, in some cases (especially for GRIB2 "
                        "inputs), this error can be resolved by setting ``eager_combine=True``."
                    )
                    if not self.eager_combine
                    else ""
                )
                raise ValueError(hints) from e

    def maybe_eager_combine(self, item: list[dict]) -> list[dict]:
        """If `self.eager_combine` is `True`, combine the kerchunk references contained in `item`
        using kerchunk's `MultiZarrToZarr.translate`. Otherwise, just pass `item` through as-is.

        In most cases, `item` will be a single-element list containing a single reference;
        typically, eager combine is not needed for these cases.

        For grib inputs containing multiple messages, however, `item` will contain > 1 elements.
        In this case, we need to eagerly combine (i.e. pre-compile) those refs into a single ref
        before they are added to the accumulator in `self.add_input`. In fact, some (but not all!)
        grib inputs containing (or filtered to give) only a single message, may still need to be
        eagerly combined. If we fail to do this for cases in which it's required, calls to
        `MultiZarrToZarr.translate` may raise chunk size mismatch errors (see also related error
        handling in `self.translate`).
        """
        # TODO: clarify which single-message grib scenarios need to be precompiled. it *may* be
        # related to what dimension is being concatenated (i.e. 'step' vs. 'time', etc.)

        # NOTE: it seems that even in cases for which it's *not* required (e.g. standard netcdf
        # datasets), eager_combine still works. for now i am not making this default/required,
        # however, because if it's not required it appears that (a) kerchunk does raise a user
        # warning, which may be misleading; and (b) it represents additional computational cost.
        return item if not self.eager_combine else [self.translate(self.to_mzz(item))]

    def create_accumulator(self):
        return None

    def add_input(self, accumulator: MultiZarrToZarr, item: list[dict]) -> MultiZarrToZarr:
        item = self.maybe_eager_combine(item)
        if not accumulator:
            references = item
        else:
            references = [self.translate(accumulator)] + item
        return self.to_mzz(references)

    def merge_accumulators(self, accumulators: Sequence[MultiZarrToZarr]) -> MultiZarrToZarr:
        references = [self.translate(a) for a in accumulators]
        return self.to_mzz(references)

    def extract_output(self, accumulator: MultiZarrToZarr) -> MultiZarrToZarr:
        return accumulator
