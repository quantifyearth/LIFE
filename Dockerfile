from carboncredits/aoh-packages as aoh

from debian:latest

RUN apt-get update -qqy && \
	apt-get install -qy \
		gdal-bin \
		gdal-data \
		libgdal-dev \
		python3 \
		python3-pip \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

# You must install numpy before anything else otherwise
# gdal's python bindings are sad
COPY requirements.txt /tmp/
RUN pip install numpy
RUN pip install -r /tmp/requirements.txt

COPY --from=aoh /iucn_modlib /usr/local/lib/python3.9/dist-packages/iucn_modlib
COPY --from=aoh /aoh /usr/local/lib/python3.9/dist-packages/aoh

COPY ./ /root/
WORKDIR /root/

RUN python3 -m pytest
RUN mypy persistence
RUN pylint persistence *.py
