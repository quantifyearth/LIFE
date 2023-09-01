
## Overview

This program to collates IUCN Red List data to help researchers calculate the likelihood of whether a species will become extinct or remain extant. It processes the habitats, elevation limits, and range polygons for entire classes of species and aggregates them into a single geo-package. This geo-package can be used to calculate and map the AOH for different species individually. Mapping the AOH of species is the first step in determining its persistence metric. 

### Key Instructions

The data for the species you are interested in needs to be **downloaded, extracted and combined** - the IUCN only lets you download the csv files and the shape files separately, but they all need to be in the *same file*.

That file name then becomes the argument that you pass in with the program when running in a cmd prompt. 

### Things to know

* The program can take a while to run - it has to read in a shapefile each time, which is usually over 1GB in size for a class. 

* If there is no shapefile data for a species that is present in one of the csv files, that species won't appear in the final file. 

* The shapefile's column for season is coded into integers from 1-5: 
	1. Resident
	2. Breeding
	3. Non-Breeding
	4. Passing
	5. Unknown
	4 and 5 are ignored and assumed to be 1 for the purposes of this program.

 

