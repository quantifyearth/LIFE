# Species selection for LIFE

LIFE is currently based on the following species selection criteria from the IUCN Redlist list of endangered species. In general we align with the guidelines set out in [Recent developments in the production of Area of Habitat (AOH) maps for terrestrial vertebrates.]() by Busana et al.

* We select from the classes AMPHIBIA, AVES, MAMMALIA, and REPTILIA.
* We exclude species that are categorised as:
    * Extinct
    * Not endangered
    * Data deficient
* Select the most recent asessment for each species.
* When selecting habitats we ignore those marked with their suitability as "marginal"
* For ranges
    * We select for origin as codes 1, 2 and 6 (Native, reintroduced, and assisted colonization)
    * LIFE generates data under both current and historic scenarios, and so the selection process for ranges is different for each scenario:
        * For current, we select under 1 and 2 (Extant and Probably Extant)
        * For historic, we select under 1, 2, 4, and 5 (Extant, Probably Extant, Possibly Extinct, and Extinct)
    * Seasonality is selecter from the categories Resident, Breeding, and non-Breeding. These are then combined in the following way:
        * For species with only a resident range, we treat them as resident only.
        * For species that are migratory (having either a breeding or non-breeding), we generate both a breeding and non-breeding range, where each is the union of the respective migratory range (if present) and the resident range if present.
* For metadata, we do the following hygine steps:
    * If elevation lower is missing, or less than the expected minimum we set it to that minimum: -500m
    * If elevation upper is missing, or over the expected maximum we set it to that maximum: 9000m
    * If the elevation lower is greater than the upper, we invert the two values
    * If the difference between the two values is less than 50m then each value is equally adjusted out from centre to ensure that they are 50m apart.

