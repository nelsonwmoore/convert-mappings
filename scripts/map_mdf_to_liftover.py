# ruff: noqa: T201
"""
Script to extract pairwise mappings from a combined CCDI Map-MDF YAML file.

This script takes a MDF-Map YAML file and extracts direct mappings from a specified
source model to the linking model (e.g., CCDIv1.7.2 to CCDIv2.1.0).

The output is a TSV file in the standard CCDI liftover mapping format.
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
import polars as pl

from convert_mappings.ccdi_liftover import extract_pairwise_mappings, load_yaml_mapping

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--map_mdf",
    "-m",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the combined mapping YAML file",
    prompt=True,
)
@click.option(
    "--source_model",
    "-s",
    type=str,
    required=True,
    help="Source model to extract (e.g., CCDIv1.7.2)",
    prompt=True,
)
@click.option(
    "--liftover_file",
    "-l",
    type=click.Path(dir_okay=False, path_type=Path),
    required=False,
    help="Output TSV file path (default: auto-generated)",
    prompt=False,
)
def main(map_mdf: Path, source_model: str, liftover_file: Path | None) -> None:
    """Extract pairwise CCDI model mappings from a combined mapping file."""
    # Load the combined mapping
    mapping_dict = load_yaml_mapping(map_mdf)

    # Extract the pairwise mappings
    try:
        mappings = extract_pairwise_mappings(mapping_dict, source_model)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort from e

    # Determine the output file path
    if liftover_file:
        output_path = liftover_file
    else:
        linking_model = mapping_dict["Source"]
        source_version = mapping_dict["Models"][source_model]["Version"]
        linking_version = mapping_dict["Models"][linking_model]["Version"]
        output_path = Path(f"{source_version}_{linking_version}_MAPPING_EXTRACTED.tsv")

    # Write the DataFrame to a TSV file
    mappings_df = pl.DataFrame(mappings)
    mappings_df.write_csv(output_path, separator="\t")
    logger.info(
        "Pairwise mapping from %s to %s saved to %s",
        source_model,
        mapping_dict["Source"],
        output_path,
    )
    logger.info("Extracted %s mappings", len(mappings))


if __name__ == "__main__":
    main()
