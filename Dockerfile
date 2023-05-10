FROM golang:bullseye AS littlejohn
RUN git clone https://github.com/carboncredits/littlejohn.git
WORKDIR littlejohn
RUN go build

from  ghcr.io/osgeo/gdal:ubuntu-small-3.6.4

COPY --from=littlejohn /go/littlejohn/littlejohn /bin/littlejohn

RUN apt-get update -qqy && \
	apt-get install -qy \
		git \
		python3-pip \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

# You must install numpy before anything else otherwise
# gdal's python bindings are sad. Pandas we full out as its slow
# to build, and this means it'll be cached
RUN pip install --upgrade pip
RUN pip install numpy
RUN pip install gdal[numpy]==3.6.4
RUN pip install pandas

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY ./ /root/
WORKDIR /root/

RUN python3 -m pytest
RUN mypy persistence
RUN pylint persistence *.py

RUN chmod 755 aohcalc.py speciesgenerator.py
