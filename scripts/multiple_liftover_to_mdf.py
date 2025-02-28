# ruff: noqa: T201
"""
Script to combine multiple CCDI liftover TSV files into a single Map-MDF YAML.

Chains the 1-1 mappings so that one can trace from an older version (e.g., 1.7.2
or 1.8.0) to the linking (target) model (here, assumed to be CCDIv2.1.0).

Any chains that do not end at the linking model are dumped into the TBD section.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml

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

# Column names as defined in your TSV files
OLD_NODE = "lift_from_node"
OLD_PROP = "lift_from_property"
OLD_VER = "lift_from_version"
NEW_NODE = "lift_to_node"
NEW_PROP = "lift_to_property"
NEW_VER = "lift_to_version"


def load_tsv(file_path: str | Path) -> pl.DataFrame:
    """Load a liftover TSV file into a polars DataFrame."""
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    return pl.read_csv(file_path, has_header=True, separator="\t")


def extract_edges(df: pl.DataFrame) -> list[dict]:
    """
    Extract mapping 'edges' from a DataFrame.

    Each edge represents a mapping from an older model/node/prop to a newer one.
    """
    edges = []
    for row in df.rows(named=True):
        edge = {
            "old_model": f"CCDIv{row[OLD_VER]}",
            "old_node": row[OLD_NODE],
            "old_prop": row[OLD_PROP],
            "new_model": f"CCDIv{row[NEW_VER]}",
            "new_node": row[NEW_NODE],
            "new_prop": row[NEW_PROP],
        }
        edges.append(edge)
    return edges


def build_chains(edges: list[dict]) -> tuple[list[list[dict]], list[list[dict]]]:
    """
    Build chains of mapping edges.

    Links rows where the destination of one edge (its new_model, new_node, new_prop)
    matches the source of another edge.

    Returns a tuple (complete_chains, conflict_chains) where:
        - complete_chains are those chains that end at the linking model.
        - conflict_chains are chains that do not reach the linking model.
    """
    # Build a set for the "destination" of an edge.
    dest_keys = {(e["new_model"], e["new_node"], e["new_prop"]) for e in edges}

    # Starting edges are those whose source is never a destination.
    starting_edges = [
        e
        for e in edges
        if (e["old_model"], e["old_node"], e["old_prop"]) not in dest_keys
    ]

    complete_chains = []
    conflict_chains = []

    # For each starting edge, follow the chain by looking for an edge whose source
    # matches the current edge's destination.
    for start in starting_edges:
        chain = [start]
        current = start
        while True:
            next_key = (current["new_model"], current["new_node"], current["new_prop"])
            # Look for an edge whose source equals this next_key.
            next_edge = next(
                (
                    e
                    for e in edges
                    if (e["old_model"], e["old_node"], e["old_prop"]) == next_key
                ),
                None,
            )
            if next_edge:
                chain.append(next_edge)
                current = next_edge
            else:
                break
        # If the final edge maps into the linking model, consider the chain complete.
        if current["new_model"] == LINKING_MODEL:
            complete_chains.append(chain)
        else:
            conflict_chains.append(chain)

    return complete_chains, conflict_chains


def update_mapping_dict(mapping_dict: dict, chain: list[dict]) -> None:
    """
    Update the mapping dictionary (to be dumped as YAML) for a complete chain.

    The chain's final edge gives the target node and property.

    For each edge in the chain, an entry is added under the target node/prop,
    keyed by the originating model.
    """
    # The final edge in the chain determines the grouping keys.
    final_edge = chain[-1]
    target_node = final_edge["new_node"]
    target_prop = final_edge["new_prop"]

    # Ensure the node exists
    if target_node not in mapping_dict["Props"]:
        mapping_dict["Props"][target_node] = {}
    # Ensure the property exists under that node
    if target_prop not in mapping_dict["Props"][target_node]:
        mapping_dict["Props"][target_node][target_prop] = {}

    # For each edge in the chain, add an entry keyed by its source model.
    for edge in chain:
        model = edge["old_model"]
        entry = {edge["old_prop"]: {"Parents": edge["old_node"]}}
        # Initialize the list for this model if not present.
        if model not in mapping_dict["Props"][target_node][target_prop]:
            mapping_dict["Props"][target_node][target_prop][model] = []
        # Avoid duplicate entries if already present.
        if entry not in mapping_dict["Props"][target_node][target_prop][model]:
            mapping_dict["Props"][target_node][target_prop][model].append(entry)

        # Also update the Models section.
        for m, ver in [
            (edge["old_model"], edge["old_model"].split("v")[-1]),
            (edge["new_model"], edge["new_model"].split("v")[-1]),
        ]:
            if m not in mapping_dict["Models"]:
                mapping_dict["Models"][m] = {"Version": ver}


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
    complete_chains, conflict_chains = build_chains(all_edges)
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
        "TBD": {},  # We'll store conflict chains here.
    }

    # Add each complete chain into the mapping.
    for chain in complete_chains:
        update_mapping_dict(mapping_dict, chain)

    # For conflict chains, record them under TBD by an index key.
    if conflict_chains:
        mapping_dict["TBD"] = {}
        for idx, chain in enumerate(conflict_chains, start=1):
            # Store a simplified representation of the chain.
            mapping_dict["TBD"][f"chain_{idx}"] = chain

    # Write the combined mapping to the output YAML file.
    output_file = Path("data/output/ccdi_combined_mapping_mdf.yml")
    with output_file.open("w") as outfile:
        yaml.dump(mapping_dict, outfile, indent=4)
    print(f"Combined mapping saved to {output_file}")


if __name__ == "__main__":
    main()
