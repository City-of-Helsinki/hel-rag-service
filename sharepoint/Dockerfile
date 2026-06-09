# SharePoint → Open WebUI pipeline container image.
#
# Build from the repo root:
#   docker build -f sharepoint/Dockerfile -t <registry>/<image>:<tag> .
#
# sites_config.yaml is NOT baked in; mount it at runtime from a ConfigMap
# (see openshift/cronjob.yaml).  No .env or credential files should be added.

FROM python:3.11-slim

WORKDIR /app

# Copy sharepoint package (crawler, config loader, pipeline runner, etc.)
COPY sharepoint/config_loader.py .
COPY sharepoint/content_processor.py .
COPY sharepoint/document_converter.py .
COPY sharepoint/main.py .
COPY sharepoint/pipeline_runner.py .
COPY sharepoint/sharepoint_client.py .
COPY sharepoint/sharepoint_crawler.py .
COPY sharepoint/requirements.txt .

# Copy Open WebUI importer as the module name used by pipeline_runner.py
COPY scripts/import_files_and_external_links.py owui_importer.py

# Install dependencies (MarkItDown is used; Docling is excluded to keep the image lean)
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "pipeline_runner.py"]
