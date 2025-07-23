# pylint: disable=C0301
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

TAXA = ["AMPHIBIA", "AVES", "MAMMALIA", "REPTILIA"]

def absolute(
    current_filename: Path,
    pnv_filename: Path,
    scenario_filename: Path,
    output_filename: Path,
) -> None:
    current = pd.read_csv(current_filename)
    pnv = pd.read_csv(pnv_filename)
    scenario = pd.read_csv(scenario_filename)

    current_cleaned = current[current.aoh_total.notnull() & current.aoh_total != 0]
    pnv_cleaned = pnv[pnv.aoh_total.notnull() & pnv.aoh_total != 0]

    merge1 = pd.merge(current_cleaned, pnv_cleaned, on=["id_no", "season"], suffixes=["_current", "_pnv"])
    merge2 = pd.merge(merge1, scenario, on=["id_no", "season"], how="left", indicator=True)
    merged = merge2[["id_no", "season", "class_name", "aoh_total_current", "aoh_total_pnv", "aoh_total", "_merge"]].copy()
    merged.aoh_total = merged.aoh_total.fillna(0)
    merged.rename(columns={"aoh_total": "aoh_total_scenario"}, inplace=True)

    merged["current_persistence"] = (merged.aoh_total_current / merged.aoh_total_pnv) ** 0.25
    merged["scenario_persistence"] = (merged.aoh_total_scenario / merged.aoh_total_pnv) ** 0.25

    merged["capped_scenario_persistence"] = np.where(merged.scenario_persistence > 1, 1, merged.scenario_persistence)
    merged["capped_current_persistence"] = np.where(merged.current_persistence > 1, 1, merged.current_persistence)

    resident = merged[merged.season == "RESIDENT"]
    breeding = merged[merged.season == "BREEDING"]
    nonbreeding = merged[merged.season == "NONBREEDING"]

    migratory = pd.merge(breeding, nonbreeding, on=["id_no"], suffixes=["_breeding", "_nonbreeding"])
    migratory["capped_scenario_persistence"] = (migratory.capped_scenario_persistence_breeding ** 0.5) * (migratory.capped_scenario_persistence_nonbreeding ** 0.5)
    migratory["capped_current_persistence"] = (migratory.capped_current_persistence_breeding ** 0.5) * (migratory.capped_current_persistence_nonbreeding ** 0.5)


    slimmed_resident = resident[["id_no", "season", "class_name", "capped_scenario_persistence", "capped_current_persistence"]].copy()
    slimmed_migratory = migratory[["id_no", "class_name_breeding", "capped_scenario_persistence", "capped_current_persistence"]].copy()
    slimmed_migratory.rename(columns={"class_name_breeding": "class_name"}, inplace=True)
    slimmed_migratory["season"] = "MIGRATORY"

    slimmed = pd.concat([slimmed_resident, slimmed_migratory])
    slimmed["extinction"] = slimmed.capped_current_persistence - slimmed.capped_scenario_persistence

    slimmed.to_csv(output_filename, index=False)

    for taxa in TAXA:
        pertaxa = slimmed[slimmed.class_name==taxa]
        print(taxa, len(pertaxa))
    print("all", len(slimmed))
    print(f"total: {slimmed.extinction.sum()}")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--current",
        type=Path,
        required=True,
        dest="current_filename",
        help="Current AoH validation summary"
    )
    parser.add_argument(
        "--pnv",
        type=Path,
        required=True,
        dest="pnv_filename",
        help="PNV AoH validation summary"
    )
    parser.add_argument(
        "--scenario",
        type=Path,
        required=True,
        dest="scenario_filename",
        help="Scenario AoH validation summary"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_filename",
        help="Destination CSV"
    )
    args = parser.parse_args()

    absolute(
        args.current_filename,
        args.pnv_filename,
        args.scenario_filename,
        args.output_filename,
    )

if __name__ == "__main__":
    main()
