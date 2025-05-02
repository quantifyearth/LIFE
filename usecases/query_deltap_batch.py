import argparse
import os
from glob import glob
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import geopandas as gpd
import shapely
from yirgacheffe.layers import RasterLayer, VectorLayer

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

def get_pixel_value(layer: RasterLayer, lat: float, lng: float) -> float:
    x, y = layer.pixel_for_latlng(lat, lng)
    return layer.read_array(x, y, 1, 1)[0][0]

def query_deltap_per_project(
    project_code: str,
    geometry: Any,
    project_mask: VectorLayer,
    corpus: str,
    outputs_path: str,
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

            deltaps_dir = os.path.join(corpus, "deltap", scenario, "0.25", klass)
            deltap_files = glob(f"{taxid}_*.tif", root_dir=deltaps_dir)
            if not deltap_files:
                continue
            assert len(deltap_files) == 1

            layer = RasterLayer.layer_from_file(os.path.join(deltaps_dir, deltap_files[0]))
            try:
                calc = layer * project_mask
                e = RasterLayer.empty_raster_layer_like(
                    calc,
                    filename=os.path.join(outputs_path, f"{project_code}_{taxid}.tif")
                )
                calc.save(e)
                species_dict[taxid] = layer
            except ValueError:
                pass

        species_keys = list(species_dict.keys())
        if len(species_keys) == 0:
            continue

        for y in range(project_mask.window.ysize):
            for x in range(project_mask.window.xsize):
                maskval = project_mask.read_array(x, y, 1, 1)[0][0]
                lat = project_mask.area.top + \
                    (project_mask.pixel_scale.ystep * y) + \
                    (project_mask.pixel_scale.ystep / 2)
                lng = project_mask.area.left + \
                    (project_mask.pixel_scale.xstep * x) + \
                    (project_mask.pixel_scale.xstep / 2)
                row = [lat, lng]
                if not maskval:
                    continue
                for species in species_keys:
                    val = species_dict[species].read_array(x, y, 1, 1)[0][0]
                    row.append(val)
                table.append(row)
        df = pd.DataFrame(table, columns=["lat", "lng"] + species_keys)
        df.to_csv(os.path.join(outputs_path, f"{project_code}_{klass}_{scenario}.csv"), index=False)

def query_deltap(
    key: str,
    inputs_path: str,
    ranges_shape_path: str,
    corpus: str,
    outputs_path: str,
):
    duckdb.install_extension("spatial")
    duckdb.load_extension("spatial")
    duckdb.query(f"create table ranges as select * from '{ranges_shape_path}'")

    os.makedirs(outputs_path, exist_ok=True)

    # Lazy way to make sure we use the right pixel scale and projection
    deltap_paths = os.path.join(corpus, "deltap")
    example_file = list(Path(deltap_paths).glob("**/*.tif"))[0]
    example = RasterLayer.layer_from_file(example_file)

    inputs_df = gpd.read_file(inputs_path)
    for _, row in inputs_df.iterrows():
        mask = VectorLayer.layer_from_file(
            inputs_path,
            f"{key} == '{row[key]}'",
            example.pixel_scale,
            example.projection
        )
        e = RasterLayer.empty_raster_layer_like(mask, filename=os.path.join(outputs_path, f"{row[key]}_mask.tif"))
        mask.save(e)

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
        type=str,
        help="GPKG with polygons of projects",
        required=True,
        dest="inputs_path"
    )
    parser.add_argument(
        '--ranges',
        type=str,
        help="related taxa ranges shapefle",
        required=True,
        dest="ranges",
    )
    parser.add_argument(
        '--corpus',
        type=str,
        help="name the output folder for lifetest",
        required=True,
        dest="corpus"
    )
    parser.add_argument(
        '--outputs',
        type=str,
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
