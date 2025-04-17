"""Script to convert a single CCDI liftover mapping TSV to MDF-Map format."""

from __future__ import annotations

from pathlib import Path

import click
import yaml

from convert_mappings.ccdi_liftover import convert_df_to_map_dict, load_tsv


@click.command()
@click.option(
    "--liftover_file",
    "-l",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="CCDI Liftover Mapping TSV file path",
    prompt=True,
)
@click.option(
    "--output_file",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output MDF-Map file path",
    prompt=True,
)
@click.option(
    "--source_model",
    "-s",
    type=str,
    required=True,
    help="Source MDF-Map version",
    prompt=True,
)
def main(liftover_file: Path, output_file: Path, source_model: str) -> None:
    """Convert a single CCDI liftover mapping TSV to MDF-Map format."""
    liftover_df = load_tsv(liftover_file)
    mdf_map_dict = convert_df_to_map_dict(liftover_df, source_model)
    yaml.dump(mdf_map_dict, output_file.open("w"), indent=4)


if __name__ == "__main__":
    main()
