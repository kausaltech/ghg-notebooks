FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip build-essential \
    libfreetype6-dev libsnappy-dev python3-dev \
    nodejs npm

RUN mkdir /build
COPY requirements.txt /build
WORKDIR /build

RUN pip3 install --no-cache-dir -r requirements.txt
RUN jupyter labextension install @jupyter-widgets/jupyterlab-manager plotlywidget jupyterlab-plotly

WORKDIR /code

ENTRYPOINT ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root"]
