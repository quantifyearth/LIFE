import argparse
import os
from pathlib import Path
from typing import Any

import pandas as pd
import geopandas as gpd
import psycopg2
import yirgacheffe as yg

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

def query_deltap_per_project(
    project_code: str,
    geometry: Any,
    project_mask: yg.YirgacheffeLayer,
    corpus: Path,
    outputs_path: Path,
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

            deltaps_dir = corpus / "deltap" / scenario / "0.25" / class_name
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
                row = [lat, lng]
                if not maskval:
                    continue
                for species in species_keys:
                    val = species_dict[species].read_array(x, y, 1, 1)[0][0]
                    row.append(val)
                table.append(row)
        df = pd.DataFrame(table, columns=["lat", "lng"] + species_keys)
        df.to_csv(outputs_path / f"{project_code}_{klass}_{scenario}.csv", index=False)

def query_deltap(
    inputs_path: Path,
    corpus: Path,
    outputs_path: Path,
):
    outputs_path.mkdir(parents=True, exist_ok=True)

    # Lazy way to make sure we use the right pixel scale and projection
    deltap_paths = corpus / "deltap"
    example_file = list(deltap_paths.glob("**/*.tif"))[0]
    with yg.read_raster(example_file) as example:


        inputs_df = gpd.read_file(inputs_path)
        for row in inputs_df.itertuples():
            with yg.read_shape_like(inputs_path, example, f"Rsrv_Code == '{row.Rsrv_Code}'") as mask:
                mask.to_geotiff(outputs_path / f"{row.Rsrv_Code}_mask.tif")
            query_deltap_per_project(str(row.Rsrv_Code), row.geometry, mask, corpus, outputs_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Species and seasonality generator.")
    parser.add_argument(
        '--inputs',
        type=Path,
        help="GPKG with polygons of projects",
        required=True,
        dest="inputs_path"
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

    query_deltap(args.inputs_path, args.corpus, args.outputs_path)

if __name__ == "__main__":
    main()
