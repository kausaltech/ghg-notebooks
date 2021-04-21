# Carbon-Neutral Helsinki notebooks

This is a collection of Jupyter notebooks to explore GHG emissions.

## Installation

Installation is easiest to do using Docker. For Ubuntu, install first docker.io and docker-compose:

```bash
apt update && apt install docker-compose docker.io
```

Add yourself to the `docker` group:

```bash
sudo gpasswd -a $USER docker
newgrp docker
```

## Starting

```bash
docker-compose up
```

Then click on the localhost URL displayed on your console to start the notebook.


## Development

First create and activate a Python virtual environment. Then, install the
dependencies and create the Jupyter Lab bundle:

```bash
pip install -r requirements.txt
jupyter labextension install @jupyter-widgets/jupyterlab-manager \
    plotlywidget jupyterlab-plotly
```

After that, you should be able to start Jupyter Lab notebook server
with:

```
jupyter lab
```
