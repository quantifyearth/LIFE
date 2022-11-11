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
