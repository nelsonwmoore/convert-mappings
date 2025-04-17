"""
Script to combine multiple CCDI liftover TSV files into a single Map-MDF YAML.

Chains the 1-1 mappings so that one can trace from an older version (e.g., 1.7.2
or 1.8.0) to the linking source model (here, assumed to be CCDIv2.1.0).

Any chains that do not end at the source model are dumped into the TBD section.
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
import yaml

from convert_mappings.ccdi_liftover import (
    build_chains,
    extract_edges,
    load_tsv,
    update_mapping_dict,
)

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--liftover_file",
    "-l",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="CCDI Liftover Mapping TSV file path",
    prompt=True,
    multiple=True,
)
@click.option(
    "--source_model",
    "-s",
    type=str,
    required=True,
    help="Source MDF-Map version; model that links the others together",
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
def main(liftover_files: list[Path], source_model: str, output_file: Path) -> None:
    """Combine multiple CCDI liftover TSV files into a single Map-MDF YAML."""
    # Gather all edges from all TSV files.
    all_edges = []
    for file_path in liftover_files:
        logger.info("Processing file: %s", file_path)
        tsv_df = load_tsv(file_path)
        edges = extract_edges(tsv_df)
        all_edges.extend(edges)

    # Build chains from the edges.
    complete_chains, conflict_chains = build_chains(all_edges, source_model)
    logger.info(
        "Found %s complete chain(s) reaching {source_model}.",
        len(complete_chains),
    )
    if conflict_chains:
        logger.warning(
            "Found %s chain(s) that did not reach %s",
            len(conflict_chains),
            source_model,
        )

    # Build the final mapping dictionary following the Map-MDF schema.
    mapping_dict = {
        "Source": source_model,
        "Models": {},
        "Props": {},
        "TBD": [],  # store conflict chains here.
    }

    # Add each complete chain into the mapping.
    for chain in complete_chains:
        update_mapping_dict(mapping_dict, chain)

    # For conflict chains, record them under TBD by an index key.
    if conflict_chains:
        for chain in conflict_chains:
            for edge in chain:
                if edge in mapping_dict["TBD"]:
                    continue
                mapping_dict["TBD"].append(edge)

    with output_file.open("w") as outfile:
        yaml.dump(mapping_dict, outfile, indent=4, sort_keys=False)
    logger.info("Combined mapping saved to %s", output_file)


if __name__ == "__main__":
    main()
