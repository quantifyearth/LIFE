BASIC=Makefile

all: aoh.csv

aoh.csv: $(BASIC) aohcalc.py trim.csv
	littlejohn -j 4 -c trim.csv -o aoh.csv -- ./venv/bin/python ./aohcalc.py

trim.csv: $(BASIC) species.csv
	head -6 species.csv > trim.csv

species.csv: $(BASIC) speciesgenerator.py /maps/4C/projects/gola_aoi.geojson
	python ./speciesgenerator.py --experiment esacci --project /maps/4C/projects/gola_aoi.geojson --output species.csv --epochs ali-jung-current,ali-jung-historic

clean:
	rm species.csv aoh.csv
