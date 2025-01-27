FROM golang:bullseye AS littlejohn
RUN git clone https://github.com/carboncredits/littlejohn.git
WORKDIR littlejohn
RUN go build

from  ghcr.io/osgeo/gdal:ubuntu-small-3.10.1

COPY --from=littlejohn /go/littlejohn/littlejohn /bin/littlejohn

RUN apt-get update -qqy && \
	apt-get install -qy \
		git \
		libpq-dev \
		python3-pip \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

# You must install numpy before anything else otherwise
# gdal's python bindings are sad. Pandas we full out as its slow
# to build, and this means it'll be cached
RUN pip install --break-system-packages numpy
RUN pip install --break-system-packages gdal[numpy]==3.10.1
RUN pip install --break-system-packages pandas

COPY requirements.txt /tmp/
RUN pip install --break-system-packages -r /tmp/requirements.txt

COPY ./ /root/
WORKDIR /root/

RUN python3 -m pytest ./tests
RUN pylint deltap prepare_layers prepare_species utils
