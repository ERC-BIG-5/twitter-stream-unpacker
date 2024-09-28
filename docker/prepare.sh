#!/bin/bash
# Create necessary directories
mkdir -p labelstudio_data/media
mkdir -p labelstudio_data/sm_imports

# Get current user's UID and GID
export UID=$(id -u)
export GID=$(id -g)

# Set ownership and permissions
sudo chown -R $UID:$GID ./labelstudio_data
sudo chmod -R 755 ./labelstudio_data

# Make init-user-db.sh executable
chmod +x init-user-db.sh

echo "Setup complete. You can now run docker-compose up."
