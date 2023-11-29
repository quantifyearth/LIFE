# Converts the cumulative deltaP rasters to values / hectare land use change
# NOT VERY EFFICIENT- THERE ARE PROBALY MUCH FASTER WAYS
rm(list = ls())
# Dependancies
library(raster)
library(terra)
library(gdata)

# Function to clean up the delta_P rasters and save them:
# 

# INPUTS:  FOLDER TO RASTERS
# LUC RASTER 
# OUTPUT: RASTER STACK
process_rasters <- function(data_dir, area_restore_path) {
  # List raster files in the directory
  rastlist <- list.files(path = data_dir, pattern = '.tif$', all.files = TRUE, full.names = TRUE)
  # Read the area_restore raster
  area_restore <- rast(area_restore_path)
  # Filter area_restore
  area_restore_filter <- ifel(area_restore < 1e4, 0, area_restore)
  area_restore_filter <- area_restore_filter / 1e4

  # Create a blank raster
  blank_raster <- rast(area_restore_filter)
  values(blank_raster) <- 0
  # Initialize an empty stack
  stack <- rast()
  for (i in 1:length(rastlist)) {
    rast_i <- rast(rastlist[i])
    # Mosaic blank_raster and rast_i, and set names
    rast_i_extent <- mosaic(blank_raster, rast_i, fun = "sum")
    # Only keep values where LUC > hectare
    rast_i_extent <- ifel(area_restore_filter != 0, rast_i_extent, 0)
    rast_i_extent_ha <- rast_i_extent / area_restore_filter
    # Set names
    names(rast_i_extent_ha) <- names(rast_i)
    # Append to the stack
    stack <- append(stack, rast_i_extent_ha)
  }
  
  return(stack)
}

# WRITE THE 0.25 restore and arable rasters
results_stack<-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/restore/0.25","/maps/results/global_analysis/rasters/area_1_arc/area/diff_restore_area.tif")
writeRaster(results_stack, filename = "/maps/results/global_analysis/outputs_mwd_processed/restore/restore_0.25.tif")

min(results_stack$amphibians)
min(results_stack$birds)

keep(process_rasters,sure = TRUE)
results_stack <-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/arable/0.25","/maps/results/global_analysis/rasters/area_1_arc/area/arable_diff_area.tif")
plot(-1*(log(results_stack$birds))
# MIGHT BE A PROBLEM WITH THIS?>
writeRaster(results_stack, filename = "/maps/results/global_analysis/outputs_mwd_processed/arable/arable_0.25.tif")
min(results_stack)
plot(results_stack$amphibians)

## WRITE OTHERS IF I WANT

# RESTORE
# 0.1
restore_0.1<-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/restore/0.1","/maps/results/global_analysis/rasters/area_1_arc/area/diff_restore_area.tif")
writeRaster(restore_0.1, filename = "/maps/results/global_analysis/outputs_mwd_processed/restore/restore_0.1.tif")
min(results_stack$amphibians)
min(results_stack$birds)

keep(process_rasters,sure = TRUE)
# 0.5 
restore_0.5<-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/restore/0.5","/maps/results/global_analysis/rasters/area_1_arc/area/diff_restore_area.tif")
writeRaster(restore_0.5, filename = "/maps/results/global_analysis/outputs_mwd_processed/restore/restore_0.5.tif")


keep(process_rasters,sure = TRUE)
#1.0
restore_1<-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/restore/1.0","/maps/results/global_analysis/rasters/area_1_arc/area/diff_restore_area.tif")
writeRaster(restore_1, filename = "/maps/results/global_analysis/outputs_mwd_processed/restore/restore_1.0.tif")
keep(process_rasters,sure = TRUE)
#gompertz 
restore_gompertz<-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/restore/gompertz","/maps/results/global_analysis/rasters/area_1_arc/area/diff_restore_area.tif")
writeRaster(restore_gompertz, filename = "/maps/results/global_analysis/outputs_mwd_processed/restore/restore_gompertz.tif")


# Conserve 
# 0.1
keep(process_rasters,sure = TRUE)
arable_0.1 <-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/arable/0.1","/maps/results/global_analysis/rasters/area_1_arc/area/arable_diff_area.tif")
hist(log(arable_0.1$birds))
writeRaster(arable_0.1, filename = "/maps/results/global_analysis/outputs_mwd_processed/arable/arable_0.1.tif")


# 0.5
keep(process_rasters,sure = TRUE)
arable_0.5 <-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/arable/0.5","/maps/results/global_analysis/rasters/area_1_arc/area/arable_diff_area.tif")
writeRaster(arable_0.5, filename = "/maps/results/global_analysis/outputs_mwd_processed/arable/arable_0.5.tif")




# 1,0
keep(process_rasters,sure = TRUE)
arable_1 <-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/arable/1.0","/maps/results/global_analysis/rasters/area_1_arc/area/arable_diff_area.tif")
writeRaster(arable_1, filename = "/maps/results/global_analysis/outputs_mwd_processed/arable/arable_1.0.tif")



# Gompertz
keep(process_rasters,sure = TRUE)
arable_gompertz <-process_rasters("/maps/results/global_analysis/outputs_mwd_summarised/arable/gompertz","/maps/results/global_analysis/rasters/area_1_arc/area/arable_diff_area.tif")
writeRaster(arable_gompertz, filename = "/maps/results/global_analysis/outputs_mwd_processed/arable/arable_gompertz.tif")
