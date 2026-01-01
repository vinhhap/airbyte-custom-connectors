FROM airbyte/python-connector-base:4.1.0

# Copy project
COPY . ./airbyte/integration_code

# Install as a package
RUN pip install ./airbyte/integration_code

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
