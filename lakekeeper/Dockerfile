# Base image
FROM quay.io/lakekeeper/catalog:latest-main

# Command to start the application
CMD ["serve"]

# Healthcheck
HEALTHCHECK --interval=1s --timeout=10s --retries=3 --start-period=3s \
  CMD /home/nonroot/iceberg-catalog healthcheck

# Expose the required port
EXPOSE 8181