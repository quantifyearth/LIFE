import argparse
import math
import os
from glob import glob
from pathlib import Path
from typing import Any

import pandas as pd
import geopandas as gpd
import psycopg2
from yirgacheffe.layers import RasterLayer, VectorLayer

MAIN_STATEMENT = """
select
    distinct on(sis_taxon_id)
    sis_taxon_id,
    taxons.scientific_name,
    taxons.class_name
from
    assessment_ranges
    left join assessments on assessments.id = assessment_ranges.assessment_id
    left join taxons on taxons.id = assessments.taxon_id
where
    assessments.latest = true
    AND taxons.class_name IN ('AVES', 'AMPHIBIA', 'MAMMALIA', 'REPTILIA')
    AND assessment_ranges.presence IN (1, 2)
    AND assessment_ranges.origin IN (1, 2, 6)
    AND assessment_ranges.seasonal IN (1, 2, 3)
    AND ST_Intersects(geom::geometry, ST_SetSRID(ST_GeomFromText(%s), 4326));
"""

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_CONFIG = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

connection = psycopg2.connect(DB_CONFIG)
cursor = connection.cursor()

def get_pixel_value(layer: RasterLayer, lat: float, lng: float) -> float:
    x = math.floor((lng - layer.area.left) / layer.pixel_scale.xstep)
    y = math.floor((lat - layer.area.top) / layer.pixel_scale.ystep)
    return layer.read_array(x, y, 1, 1)[0][0]

def query_deltap_per_project(
    project_code: str,
    geometry: Any,
    project_mask: VectorLayer,
    corpus: str,
    outputs_path: str,
):
    cursor.execute(MAIN_STATEMENT, (geometry.wkt,))
    allinfo = list(cursor.fetchall())
    print(project_code)
    print(len(allinfo))

    scenario = "arable"

    for klass in ('AVES', 'AMPHIBIA', 'MAMMALIA', 'REPTILIA'):
        table = []
        species_dict = {}

        for taxid, _, class_name in allinfo:
            if class_name != klass:
                continue

            deltaps_dir = os.path.join(corpus, "deltap", scenario, "0.25", class_name)
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
    inputs_path: str,
    corpus: str,
    outputs_path: str,
):
    os.makedirs(outputs_path, exist_ok=True)

    # Lazy way to make sure we use the right pixel scale and projection
    deltap_paths = os.path.join(corpus, "deltap")
    example_file = list(Path(deltap_paths).glob("**/*.tif"))[0]
    example = RasterLayer.layer_from_file(example_file)


    inputs_df = gpd.read_file(inputs_path)
    for row in inputs_df.itertuples():
        mask = VectorLayer.layer_from_file(
            inputs_path,
            f"Rsrv_Code == '{row.Rsrv_Code}'",
            example.pixel_scale,
            example.projection
        )
        e = RasterLayer.empty_raster_layer_like(mask, filename=os.path.join(outputs_path, f"{row.Rsrv_Code}_mask.tif"))
        mask.save(e)

        query_deltap_per_project(row.Rsrv_Code, row.geometry, mask, corpus, outputs_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Species and seasonality generator.")
    parser.add_argument(
        '--inputs',
        type=str,
        help="GPKG with polygons of projects",
        required=True,
        dest="inputs_path"
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

    query_deltap(args.inputs_path, args.corpus, args.outputs_path)

if __name__ == "__main__":
    main()
