Use the official Python base image
FROM python:3.9-buster

# Set the working directory in the container
WORKDIR /app

# Ensure non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y build-essential openjdk-11-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the Python dependencies with verbose output
#RUN pip install --no-cache-dir -r requirements.txt --verbose

# Copy the entire project into the container
COPY . .

# Set environment variables for PyFlink (if needed)
ENV FLINK_HOME /usr/local/flink
ENV PATH $FLINK_HOME/bin:$PATH

# Expose port (if your application runs on a specific port)
EXPOSE 8081

# Specify the command to run on container start
CMD ["python", "app.py"]
