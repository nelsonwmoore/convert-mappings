"""Script to convert CCDI liftover mapping TSV to MDF-Map format."""

from __future__ import annotations

from pathlib import Path

import yaml

from convert_mappings.ccdi_liftover import convert_df_to_map_dict, load_tsv

# constants
CCDI_LIFTOVER_TSV = Path("1.7.2_1.9.1_MAPPING_20240718.tsv")
OUTPUT_MDF_MAP = Path("ccdi_1.7.2_1.9.1_mapping_mdf.yml")
SOURCE = "CCDIv1.9.1"


def main() -> None:
    """Do stuff."""
    liftover_df = load_tsv(CCDI_LIFTOVER_TSV)
    mdf_map_dict = convert_df_to_map_dict(liftover_df, SOURCE)
    yaml.dump(mdf_map_dict, OUTPUT_MDF_MAP.open("w"), indent=4)


if __name__ == "__main__":
    main()
