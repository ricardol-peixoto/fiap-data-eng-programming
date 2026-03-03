FROM apache/spark:4.0.0

# Install Python
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Create working directory
RUN mkdir -p /opt/spark/app
WORKDIR /opt/spark/app

# Copy your files
COPY . .

# Install dependencies
RUN pip install -r requirements.txt

# Keep container running
CMD ["tail", "-f", "/dev/null"]