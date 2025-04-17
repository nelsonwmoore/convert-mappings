#!/usr/bin/env python

"""Split CCDI liftover mapping file into separate 'node' and 'relationship' files."""

from pathlib import Path

import click
import polars as pl

from convert_mappings.ccdi_liftover import load_tsv


@click.command()
@click.option(
    "--liftover_tsv",
    type=click.Path(dir_okay=False, path_type=Path),
    help="CCDI Liftover Mapping TSV file path",
    prompt=True,
)
def main(liftover_tsv: Path):
    """Separate node and relationship mappings into separate files."""
    tsv_path = liftover_tsv.resolve()
    tsv_df = load_tsv(tsv_path)

    relationships_df = tsv_df.filter(
        pl.col("lift_from_property").str.contains(r"\.")
        | pl.col("lift_to_property").str.contains(r"\.")
    )

    # Filter for rows that do not have a period in either column.
    nodes_df = tsv_df.filter(
        ~(
            pl.col("lift_from_property").str.contains(r"\.")
            | pl.col("lift_to_property").str.contains(r"\.")
        )
    )
    relationships_path = tsv_path.parent / Path(tsv_path.stem + "_relationships.tsv")
    nodes_path = tsv_path.parent / Path(tsv_path.stem + "_nodes.tsv")
    relationships_df.write_csv(relationships_path, separator="\t")
    nodes_df.write_csv(nodes_path, separator="\t")


if __name__ == "__main__":
    main()
