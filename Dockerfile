ARG  python_pipenv_build_image=europe-west2-docker.pkg.dev/ons-ci-rm/docker/python-pipenv:3.12
# hadolint ignore=DL3006
FROM ${python_pipenv_build_image} AS build

ENV PIPENV_VENV_IN_PROJECT=1

WORKDIR /home/autoprocessor
COPY Pipfile* /home/autoprocessor/

RUN /root/.local/bin/pipenv sync

FROM python:3.12.12-slim@sha256:dc9e92fcdc085ad86dda976f4cfc58856dba33a438a16db37ff00151b285c8ca

RUN groupadd -g 1000 autoprocessor && \
    useradd -r --create-home -u 1000 -g autoprocessor autoprocessor

WORKDIR /home/autoprocessor
CMD ["/home/autoprocessor/venv/bin/python", "run.py"]

RUN mkdir -v /home/autoprocessor/venv /home/autoprocessor/.postgresql && \
    chown autoprocessor:autoprocessor /home/autoprocessor/venv /home/autoprocessor/.postgresql

COPY --chown=autoprocessor:autoprocessor --from=build /home/autoprocessor/.venv/ /home/autoprocessor/venv/
COPY --chown=autoprocessor:autoprocessor . /home/autoprocessor/

USER autoprocessor
