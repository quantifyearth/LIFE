from debian:latest

RUN apt-get update -qqy && \
	apt-get install -qy \
		gdal-bin \
		gdal-data \
		git \
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

COPY ./ /root/
WORKDIR /root/

RUN python3 -m pytest
RUN mypy persistence
RUN pylint persistence *.py
