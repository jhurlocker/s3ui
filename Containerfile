# Use Red Hat's Universal Base Image 9 for Python 3.9
FROM registry.access.redhat.com/ubi9/python-39

# Switch to the root user to perform installations and set permissions
USER root

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies as root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Create the directory for persistent configuration and set permissions
# This allows the non-root user (1001) running the app to write to this directory,
# because it will belong to the root group (0).
RUN mkdir -p /data/config && \
    chown -R 1001:0 /data/config && \
    chmod -R g+w /data/config && \
    # Also ensure the app directory itself has the correct group ownership
    chown -R 1001:0 /app && \
    chmod -R g+w /app

# Expose the port the app runs on
EXPOSE 5001

# Switch to a non-privileged user for running the application
USER 1001

# Define the command to run the application
CMD ["python", "app.py"]
