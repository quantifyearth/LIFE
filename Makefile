BASIC=Makefile

all: aoh.csv

aoh.csv: $(BASIC) aohcalc.py trim.csv
	littlejohn -j 4 -c trim.csv -o aoh.csv -- ./venv/bin/python ./aohcalc.py --experiment esacci

trim.csv: $(BASIC) species.csv
	head -5 species.csv > trim.csv

species.csv: $(BASIC) speciesgenerator.py ../../data/projects/gola_aoi.geojson
	python ./speciesgenerator.py --experiment esacci --project ../../data/projects/gola_aoi.geojson --output species.csv

clean:
	rm species.csv aoh.csv