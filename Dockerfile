FROM python:3.12-slim

WORKDIR /app

# Copy package metadata and source for installation
COPY pyproject.toml .
COPY src/ src/
COPY scripts/ scripts/
# Only config/examples/ is tracked in git; real configs are gitignored
COPY config/ config/

# Install package and dependencies
RUN pip install --no-cache-dir .

# Create default directories for volumes
RUN mkdir -p /app/data /app/import /app/credentials

ENTRYPOINT ["momoney"]
CMD ["watch"]
