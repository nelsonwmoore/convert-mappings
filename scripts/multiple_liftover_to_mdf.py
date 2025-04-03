# ruff: noqa: T201
"""
Script to combine multiple CCDI liftover TSV files into a single Map-MDF YAML.

Chains the 1-1 mappings so that one can trace from an older version (e.g., 1.7.2
or 1.8.0) to the linking (target) model (here, assumed to be CCDIv2.1.0).

Any chains that do not end at the linking model are dumped into the TBD section.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from convert_mappings.ccdi_liftover import (
    build_chains,
    extract_edges,
    load_tsv,
    update_mapping_dict,
)

# List of input TSV files (adjust the paths as needed)
FILE_DIR = Path("data/source/ccdi_liftover_mappings")
FILE_PATHS = [
    FILE_DIR / Path("1.7.2_1.9.1_MAPPING_20240718.tsv"),
    FILE_DIR / Path("ccdi-model_1.9.1_ccdi-model_2.0.0_MAPPING_20241121.tsv"),
    FILE_DIR / Path("ccdi-model_1.8.0_ccdi-model_1.9.1_MAPPING_20240904.tsv"),
    FILE_DIR / Path("ccdi-model_2.0.0_ccdi-model_2.1.0_MAPPING_20250206.tsv"),
]

# Define the linking (target) model version.
LINKING_MODEL_VERSION = "2.1.0"
LINKING_MODEL = f"CCDIv{LINKING_MODEL_VERSION}"


def main() -> None:
    """Do stuff."""
    # Gather all edges from all TSV files.
    all_edges = []
    for file_path in FILE_PATHS:
        print(f"Processing file: {file_path}")
        tsv_df = load_tsv(file_path)
        edges = extract_edges(tsv_df)
        all_edges.extend(edges)

    # Build chains from the edges.
    complete_chains, conflict_chains = build_chains(all_edges, LINKING_MODEL)
    print(f"Found {len(complete_chains)} complete chain(s) reaching {LINKING_MODEL}.")
    if conflict_chains:
        print(
            f"Found {len(conflict_chains)} chain(s) that did not reach {LINKING_MODEL}",
        )

    # Build the final mapping dictionary following the Map-MDF schema.
    mapping_dict = {
        "Source": LINKING_MODEL,
        "Models": {},
        "Props": {},
        "TBD": [],  # We'll store conflict chains here.
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

    # Write the combined mapping to the output YAML file.
    output_file = Path("data/output/ccdi_combined_mapping_mdf_20250403.yml")
    with output_file.open("w") as outfile:
        yaml.dump(mapping_dict, outfile, indent=4, sort_keys=False)
    print(f"Combined mapping saved to {output_file}")


if __name__ == "__main__":
    main()
