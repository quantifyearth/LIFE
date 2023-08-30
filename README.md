
Code for calculating persistence values.

Originally derived from and using IUCN modlib and aoh lib by Daniele Baisero.

## Pipeline

The code is designed to run as a series of independant stages to minimise re-running code. The stages currently are:

1. Input generation: running speciesgenerator.py will generate a CSV list of species/seasonality/experiment tuples.
2. AoH calculation: using a tool like [littlejohn](https://github.com/carboncredits/littlejohn) you can then process each line of the generated data. This will create a new CSV file that has the inputs plus the area in.
3. Persistence calculation: TODO - add script that processes the output of stage 2 to generate persistence values in CSV.

This is currently encoded in the included makefile.

## Configuration

The main program run.py takes configuration from a json file, which should be called `config.json` or specified using the --config parameter. The contents of the file should be like this:

```
{
    "iucn": {
        "api_key": "YOUR_IUCN_API_KEY"
    },
    "experiments": {
        "ae491-jung": {
            "translator": "jung",
            "habitat": "S:\\aoh_trial\\jung_aoh_basemaps\\HabMap_merge.tif",
            "elevation": "S:\\aoh_trial\\jung_aoh_basemaps\\Modlib_DEM_merge.tif",
            "area": "S:\\aoh_trial\\jung_aoh_basemaps\\small.tiff",
            "range": "S:\\aoh_trial\\mammals_terrestrial_filtered_collected_fix.gpkg",
            "iucn_batch": "S:\\4C\\data\\aoh_trial\\MAMMALS"
        },
        "gpu_tests": {
            "translator": "esacci",
            "habitat": "S:\\aoh_trial\\esacci_aoh_basemaps\\esacci_2020.tif",
            "elevation": "S:\\aoh_trial\\esacci_aoh_basemaps\\esacci_dem.tif",
            "area": "S:\\aoh_trial\\esacci_aoh_basemaps\\small_area.tif",
            "range": "S:\\aoh_trial\\mammals_terrestrial_filtered_collected_fix.gpkg"
        }
    }
}
```

| Key | Optional | Meaning |
| --- | -------- | ------- |
| iucn | yes | Contains IUCN API access data. |
| api_key | yes | Your key for accessing the IUCN redlist API. |
| experiments | no | A dictionary of data required to run an experiment. You use the --experiment option on run.py to select the one you want to use for a particular invocation. |
| translator | no | Which translator should be used to convert the range map data to match the raster map data. Valid values are "jung" and "esacci". |
| habitat | no | Raster habitat map file location |
| elevation | no | Raster elevation map file location |
| area | yes | Raster area of pixel map file location. If not provided you'll get a count of pixels rather than a total area. |
| range | no | Vector species range map file location |
| iucn_batch | yes | The location of canned/pre-downloaded IUCN data. If present this will be used in preference of doing API lookings. |


## GPU Support

CUDA support is provided if cupy is installed.


# H3AreaCalculator

This is the script for calculating the area of a species AoH to individual hex tiles, based on the [H3 tile system](https://h3geo.org/).

## Usage

To run for a set of species do:

```
$ python ./calculate.py CURRENT_RASTERS_DIR RANGE_FILE OUTPUT_DIR
```

Where the arguments are:

* CURRENT_RASTERS_DIR - A directory of AoH GeoTIFFs, where each pixel contains an area value of the habitate in the land area covered by that pixel for the species. We currently assume the ID of the species is in the filename.
* RANGE_FILE - A vertor range file that contains the range for all species in the CURRENT_RASTERS_DIR
* OUTPUT_DIR - A directory where to write the output

The output is a CSV file per species that contains the area per tile. If you want to see what those look like you can load them into [Kepler GL](https://kepler.gl/).

## Notes

This is a test of using H3 as the basis for doing equal area calculatins on non-uniform map projections. Currently it just calculates the area of habitat per h3 tile.

The key so far has been using parallelism to make things work well, but avoiding using GDAL in any concurrent context, as GDAL is both thread-unsafe and leaks memory. Thus we use python multiprocessing for parallelism, which uses a new process per worker, and use a new GDAL context in each worker so we don't accumlate leaked memory - it goes away when the worker goes away.

Working out the H3 files to use is a two stage process, again due to GDAL concurrecy limitations. We first do a single threaded appraoch to get all the polygons for a species range, and then once we have the polygon data we can then parallelise turning those polygons into H3 tile IDs. If the individual polygons are very big (often we see one very large polygon and a lot of smaller satellites), we split up the large polygon into smaller sections to aid parallelism. The current way of doing this is naive (just bands of 2 degrees longitude) but is enough to get acceptable performance for the proof of concept.

Then we just process each hex tile in as many concurrent workers as there are CPU cores on the machine. I suspect here we hit an overhead based on having to repeatedly search the GDAL raster file for data, as we see on hipp we go from 10k hex tiles per second to 1k hex tiles per second as the raster gets significantly larger. An optimisation to investigate is opening the raster once and then using shared memory between the workers. But even without this, the largest example we have takes just 4.4 hours on hipp, with others being considerably less. So again, for a proof of concept, this seems a good place to be.

