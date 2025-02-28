"""Script to convert CCDI liftover mapping TSV to MDF-Map format."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml

# Path constants
CCDI_LIFTOVER_TSV = Path("1.7.2_1.9.1_MAPPING_20240718.tsv")
OUTPUT_MDF_MAP = Path("ccdi_1.7.2_1.9.1_mapping_mdf.yml")
# Liftover column constants
OLD_NODE = "lift_from_node"
OLD_PROP = "lift_from_property"
OLD_VER = "lift_from_version"
NEW_NODE = "lift_to_node"
NEW_PROP = "lift_to_property"
NEW_VER = "lift_to_version"
# Other constants
HANDLE = "CCDI"
SOURCE = "CCDIv1.9.1"


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


def convert_df_to_map_dict(df: pl.DataFrame) -> dict:
    """Convert polars dataframe of liftover mappings to MDF mapping dict."""
    mdf_map = {
        "Source": SOURCE,
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


def main() -> None:
    """Do stuff."""
    liftover_df = load_tsv(CCDI_LIFTOVER_TSV)
    mdf_map_dict = convert_df_to_map_dict(liftover_df)
    yaml.dump(mdf_map_dict, OUTPUT_MDF_MAP.open("w"), indent=4)


if __name__ == "__main__":
    main()
