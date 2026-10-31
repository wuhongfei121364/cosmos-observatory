# Stdlib-only Python service: no pip install needed, builds offline from the base image.
FROM python:3.11-slim
WORKDIR /srv
COPY app/ /srv/app/
COPY scripts/ /srv/scripts/
ENV DATA_DIR=/data PORT=8000 PYTHONUNBUFFERED=1
VOLUME ["/data"]
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/api/health',timeout=2)"
CMD ["python", "/srv/app/server.py"]
