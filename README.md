Code for calculating persistence values.

Derived from and using IUCN modlib and aoh lib by Daniele Baisero.


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

