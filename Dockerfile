FROM pangeo/pangeo-notebook:2020.08.24
ENV PATH="/srv/conda/envs/notebook/bin:${PATH}"
USER ${NB_USER}
ENV JUPYTERHUB_USER=pangeoforge
ENV TZ=UTC
RUN python -m pip install prefect[kubernetes]==0.13.0 pygithub
COPY . /pangeo-forge
RUN python -m pip install --no-deps /pangeo-forge
