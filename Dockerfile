from osgeo/gdal:ubuntu-small-latest

RUN apt-get update -qqy && \
	apt-get install -qy \
		git \
		python3-pip \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

# You must install numpy before anything else otherwise
# gdal's python bindings are sad
COPY requirements.txt /tmp/
RUN pip install --upgrade pip
RUN pip install numpy
RUN pip install -r /tmp/requirements.txt

COPY ./ /root/
WORKDIR /root/

RUN python3 -m pytest
RUN mypy persistence
RUN pylint persistence *.py
