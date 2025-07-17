ARG  python_pipenv_build_image=europe-west2-docker.pkg.dev/ons-ci-rm/docker/python-pipenv:3.12
# hadolint ignore=DL3006
FROM ${python_pipenv_build_image} AS build

ENV PIPENV_VENV_IN_PROJECT=1

WORKDIR /home/autoprocessor
COPY Pipfile* /home/autoprocessor/

RUN /root/.local/bin/pipenv sync

FROM python:3.12.10-slim@sha256:bae1a061b657f403aaacb1069a7f67d91f7ef5725ab17ca36abc5f1b2797ff92

RUN groupadd -g 1000 autoprocessor && \
    useradd -r --create-home -u 1000 -g autoprocessor autoprocessor

WORKDIR /home/autoprocessor
CMD ["/home/autoprocessor/venv/bin/python", "run.py"]

RUN mkdir -v /home/autoprocessor/venv /home/autoprocessor/.postgresql && \
    chown autoprocessor:autoprocessor /home/autoprocessor/venv /home/autoprocessor/.postgresql

COPY --chown=autoprocessor:autoprocessor --from=build /home/autoprocessor/.venv/ /home/autoprocessor/venv/
COPY --chown=autoprocessor:autoprocessor . /home/autoprocessor/

USER autoprocessor
