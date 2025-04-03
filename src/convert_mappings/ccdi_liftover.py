"""Functions for working with CCDI liftover mappings."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml

# Liftover column constants
OLD_NODE = "lift_from_node"
OLD_PROP = "lift_from_property"
OLD_VER = "lift_from_version"
NEW_NODE = "lift_to_node"
NEW_PROP = "lift_to_property"
NEW_VER = "lift_to_version"
HANDLE = "CCDI"


def load_tsv(file_path: str | Path) -> pl.DataFrame:
    """Load CCDI liftover TSV into polars dataframe."""
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    return pl.read_csv(file_path, has_header=True, separator="\t")


def update_model_info(row: dict[str, str], map_dict: dict) -> None:
    """Update model and version info from a row in an MDF mapping dict."""
    old_model = "".join([HANDLE, "v", row[OLD_VER]])
    new_model = "".join([HANDLE, "v", row[NEW_VER]])
    if old_model not in map_dict["Models"]:
        map_dict["Models"][old_model] = {"Version": row[OLD_VER]}
    if new_model not in map_dict["Models"]:
        map_dict["Models"][new_model] = {"Version": row[NEW_VER]}


def update_node_info(row: dict[str, str], map_dict: dict) -> None:
    """Update node info from a row in an MDF mapping dict."""
    if not row[NEW_NODE]:
        map_dict["TBD"][row[OLD_NODE]] = row
    elif row[NEW_NODE] not in map_dict["Props"]:
        map_dict["Props"][row[NEW_NODE]] = {}
    # elif row[OLD_NODE] not in map_dict["Props"] and row[OLD_NODE] != row[NEW_NODE]:
    #     map_dict["TBD"][row[OLD_NODE]] = {}


def update_prop_info(row: dict[str, str], map_dict: dict) -> None:
    """Update property info from a row in an MDF mapping dict."""
    if not row[NEW_NODE]:
        return
    if row[NEW_PROP] not in map_dict["Props"][row[NEW_NODE]]:
        old_model = "".join([HANDLE, "v", row[OLD_VER]])
        map_dict["Props"][row[NEW_NODE]][row[NEW_PROP]] = {
            old_model: [],
        }
        if (
            row[OLD_PROP]
            and row[OLD_PROP]
            not in map_dict["Props"][row[NEW_NODE]][row[NEW_PROP]][old_model]
        ):
            map_dict["Props"][row[NEW_NODE]][row[NEW_PROP]][old_model].append(
                {row[OLD_PROP]: {"Parents": row[OLD_NODE]}},
            )


def convert_df_to_map_dict(df: pl.DataFrame, source_model: str) -> dict:
    """Convert polars dataframe of liftover mappings to MDF mapping dict."""
    mdf_map = {
        "Source": source_model,
        "Models": {},
        "Props": {},
        "TBD": {},
    }
    for row in df.rows(named=True):
        update_model_info(row, mdf_map)
        update_node_info(row, mdf_map)
        update_prop_info(row, mdf_map)

    print(mdf_map)

    return mdf_map


def extract_edges(df: pl.DataFrame) -> list[dict]:
    """
    Extract mapping 'edges' from a DataFrame.

    Each edge represents a mapping from an older model/node/prop to a newer one.
    """
    edges = []
    for row in df.rows(named=True):
        edge = {
            "old_node": row[OLD_NODE],
            "old_prop": row[OLD_PROP],
            "old_model": f"CCDIv{row[OLD_VER]}",
            "new_node": row[NEW_NODE],
            "new_prop": row[NEW_PROP],
            "new_model": f"CCDIv{row[NEW_VER]}",
        }
        edges.append(edge)
    return edges


def build_chains(
    edges: list[dict], linking_model: str
) -> tuple[list[list[dict]], list[list[dict]]]:
    """
    Build chains of mapping edges.

    Links rows where the destination of one edge (its new_model, new_node, new_prop)
    matches the source of another edge.

    Returns a tuple (complete_chains, conflict_chains) where:
        - complete_chains are those chains that end at the linking model.
        - conflict_chains are chains that do not reach the linking model.
    """
    # Build a set for the "destination" of an edge.
    # Only include destination keys that are fully mapped.
    dest_keys = {
        (e["new_model"], e["new_node"], e["new_prop"])
        for e in edges
        if e["new_model"] and e["new_node"] and e["new_prop"]
    }

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
            if not current["new_node"] or not current["new_prop"]:
                break
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
        if (
            current["new_model"] == linking_model
            and current["new_node"]
            and current["new_prop"]
        ):
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


def load_yaml_mapping(file_path: str | Path) -> dict:
    """Load the combined mapping YAML file."""
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    with file_path.open("r") as infile:
        return yaml.safe_load(infile)


def extract_pairwise_mappings(mapping_dict: dict, source_model: str) -> list[dict]:
    """
    Extract direct mappings from source_model to the linking model.

    Returns a list of dictionaries, each representing a row in the output TSV.
    """
    # Get the linking model from the mapping dict
    linking_model = mapping_dict["Source"]
    linking_version = mapping_dict["Models"][linking_model]["Version"]

    # Verify the source model exists
    if source_model not in mapping_dict["Models"]:
        raise ValueError(f"Source model {source_model} not found in mapping file")

    source_version = mapping_dict["Models"][source_model]["Version"]

    # Extract the pairwise mappings
    mappings = []

    for target_node, props in mapping_dict["Props"].items():
        for target_prop, model_mappings in props.items():
            # Check if we have mappings for our source model
            if source_model in model_mappings:
                for mapping in model_mappings[source_model]:
                    for source_prop, details in mapping.items():
                        source_node = details["Parents"]
                        row = {
                            OLD_NODE: source_node,
                            OLD_PROP: source_prop,
                            OLD_VER: source_version,
                            NEW_NODE: target_node,
                            NEW_PROP: target_prop,
                            NEW_VER: linking_version,
                        }
                        mappings.append(row)
            else:
                row = {
                    OLD_NODE: "",
                    OLD_PROP: "",
                    OLD_VER: source_version,
                    NEW_NODE: target_node,
                    NEW_PROP: target_prop,
                    NEW_VER: linking_version,
                }

    for edge in mapping_dict["TBD"]:
        if edge["old_model"] == source_model:
            row = {
                "lift_from_node": edge["old_node"],
                "lift_from_property": edge["old_prop"],
                "lift_from_version": source_version,
                "lift_to_node": "",
                "lift_to_property": "",
                "lift_to_version": linking_version,
            }
            mappings.append(row)

    return mappings
