FROM golang:latest AS reclaimer
RUN git clone https://github.com/quantifyearth/reclaimer.git
WORKDIR reclaimer
RUN go build

FROM  ghcr.io/osgeo/gdal:ubuntu-small-3.12.2

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000

RUN apt-get update -qqy && \
	apt-get install -qy \
		git \
		libpq-dev \
		python3-pip \
        python3-venv \
        python3-dev \
		r-base-core r-base-dev \
        graphviz \
        direnv \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

COPY --from=reclaimer /go/reclaimer/reclaimer /bin/reclaimer

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir gdal[numpy]==3.12.2

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./ /root/
WORKDIR /root/

RUN python3 -m pytest ./tests
RUN pylint deltap prepare_layers prepare_species utils

# --- Non-root dev user ---
# UID/GID come from docker-compose.yml (build.args) so they match the host
# owner of the bind-mounted repo.
RUN groupadd -g ${USER_GID} ${USERNAME} \
    && useradd -m -s /bin/bash -u ${USER_UID} -g ${USER_GID} ${USERNAME}
USER ${USERNAME}
WORKDIR /home/${USERNAME}/workspace

CMD ["/bin/bash"]