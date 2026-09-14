import argparse
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import geopandas as gpd
import shapely
import yirgacheffe as yg

MAIN_STATEMENT = """
SELECT
    id_no
FROM
    ranges
WHERE
    presence IN (1, 2)
    AND origin IN (1, 2, 6)
    AND seasonal IN (1, 2, 3, 5)
    AND ST_Intersects(geometry, ST_GeomFromText(?));
"""

def query_deltap_per_project(
    project_code: str,
    geometry: Any,
    project_mask: yg.YirgacheffeLayer,
    corpus: Path,
    outputs_path: Path,
):

    con = duckdb.connect(":default:")
    allinfo = con.execute(MAIN_STATEMENT, (shapely.to_wkt(geometry),)).fetchall()

    print(project_code)
    print(len(allinfo))

    scenario = "arable"

    for klass in ('AVES', 'AMPHIBIA', 'MAMMALIA', 'REPTILIA'):
        table = []
        species_dict = {}

        for row in allinfo:
            taxid = row[0]

            deltaps_dir = corpus / "deltap" / scenario / "0.25" / klass
            deltap_files = list(deltaps_dir.glob(f"{taxid}_*.tif"))
            if not deltap_files:
                continue
            assert len(deltap_files) == 1

            layer = yg.read_raster(deltap_files[0])
            try:
                calc = layer * project_mask
                calc.to_geotiff(outputs_path / f"{project_code}_{taxid}.tif")
                species_dict[taxid] = layer
            except ValueError:
                pass

        species_keys = list(species_dict.keys())
        if len(species_keys) == 0:
            continue

        project_mask_projection = project_mask.projection
        assert project_mask_projection is not None

        for y in range(project_mask.dimensions[1]):
            for x in range(project_mask.dimensions[0]):
                maskval = project_mask.read_array(x, y, 1, 1)[0][0]
                lat = project_mask.area.top + \
                    (project_mask_projection.ystep * y) + \
                    (project_mask_projection.ystep / 2)
                lng = project_mask.area.left + \
                    (project_mask_projection.xstep * x) + \
                    (project_mask_projection.xstep / 2)
                result_row = [lat, lng]
                if not maskval:
                    continue
                for species in species_keys:
                    val = species_dict[species].read_array(x, y, 1, 1)[0][0]
                    result_row.append(val)
                table.append(result_row)
        df = pd.DataFrame(table, columns=["lat", "lng"] + species_keys)
        df.to_csv(outputs_path / f"{project_code}_{klass}_{scenario}.csv", index=False)

def query_deltap(
    key: str,
    inputs_path: Path,
    ranges_shape_path: Path,
    corpus: Path,
    outputs_path: Path,
):
    duckdb.install_extension("spatial")
    duckdb.load_extension("spatial")
    duckdb.query(f"create table ranges as select * from '{ranges_shape_path}'")

    outputs_path.mkdir(parents=True, exist_ok=True)

    # Lazy way to make sure we use the right pixel scale and projection
    deltap_paths = corpus / "deltap"
    example_file = list(deltap_paths.glob("**/*.tif"))[0]
    with yg.read_raster(example_file) as example:
        inputs_df = gpd.read_file(inputs_path)
        for _, row in inputs_df.iterrows():
            with yg.read_shape_like(inputs_path, example, where_filter=f"{key} == '{row[key]}'") as mask:
                mask.to_geotiff(outputs_path / f"{row[key]}_mask.tif")

            query_deltap_per_project(row[key], row.geometry, mask, corpus, outputs_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Species and seasonality generator.")
    parser.add_argument(
        '--key',
        type=str,
        help="unique key for projects",
        required=True,
        dest="key"
    )
    parser.add_argument(
        '--inputs',
        type=Path,
        help="GPKG with polygons of projects",
        required=True,
        dest="inputs_path"
    )
    parser.add_argument(
        '--ranges',
        type=Path,
        help="related taxa ranges shapefle",
        required=True,
        dest="ranges",
    )
    parser.add_argument(
        '--corpus',
        type=Path,
        help="name the output folder for lifetest",
        required=True,
        dest="corpus"
    )
    parser.add_argument(
        '--outputs',
        type=Path,
        help="name of output directory for csvs",
        required=True,
        dest="outputs_path"
    )
    args = parser.parse_args()

    query_deltap(
        args.key,
        args.inputs_path,
        args.ranges,
        args.corpus,
        args.outputs_path
    )

if __name__ == "__main__":
    main()
