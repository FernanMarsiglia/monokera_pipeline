#!/bin/bash

###############################################################################
# Setup script for EC2 instance - Run this once to prepare the server
# Usage: bash setup-ec2.sh
###############################################################################

set -e

compose_cmd() {
    if docker compose version &> /dev/null; then
        echo "docker compose"
    elif command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    else
        echo ""
    fi
}

echo "🚀 Starting EC2 setup for monokera-pipeline..."

# Update system packages
echo "📦 Updating system packages..."
sudo yum update -y || sudo apt-get update -y

# Install Docker
echo "🐳 Installing Docker..."
if command -v docker &> /dev/null; then
    echo "✅ Docker already installed"
else
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo usermod -aG docker ec2-user
    rm get-docker.sh
fi

# Install Docker Compose
echo "🐳 Installing Docker Compose..."
if [ -n "$(compose_cmd)" ]; then
    echo "✅ Docker Compose already installed"
else
    mkdir -p ~/.docker/cli-plugins
    ARCH=$(uname -m)
    if [ "$ARCH" = "x86_64" ]; then
        COMPOSE_ARCH="x86_64"
    elif [ "$ARCH" = "aarch64" ]; then
        COMPOSE_ARCH="aarch64"
    else
        echo "❌ Unsupported architecture for Docker Compose: $ARCH"
        exit 1
    fi

    curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-${COMPOSE_ARCH}" \
      -o ~/.docker/cli-plugins/docker-compose
    chmod +x ~/.docker/cli-plugins/docker-compose
fi

# Install Git
echo "📝 Installing Git..."
sudo yum install git -y || sudo apt-get install git -y

# Setup SSH for GitHub
echo "🔐 Setting up SSH for GitHub..."
if [ ! -f ~/.ssh/id_rsa ]; then
    echo "Generate a new SSH key for GitHub:"
    ssh-keygen -t ed25519 -C "ec2-deploy@github" -f ~/.ssh/id_rsa -N ""
    echo ""
    echo "📋 Add this public key to GitHub (Settings → SSH Keys):"
    cat ~/.ssh/id_rsa.pub
else
    echo "✅ SSH key already exists at ~/.ssh/id_rsa"
fi

# Configure Git
echo "⚙️  Configuring Git..."
git config --global user.name "EC2 Deploy Bot" || true
git config --global user.email "deploy@ec2.local" || true

# Create app directory
APP_PATH="/home/ec2-user/monokera_pipeline"
echo "📁 Creating app directory at $APP_PATH..."
mkdir -p "$APP_PATH"
cd "$APP_PATH"

# Clone repository
if [ ! -d .git ]; then
    echo "🔄 Cloning repository..."
    git clone git@github.com:FernanMarsiglia/monokera_pipeline.git .
else
    echo "✅ Repository already cloned"
fi

# Create environment file (.env)
if [ ! -f .env ]; then
    echo "📄 Creating .env file (update with your AWS credentials)..."
    cat > .env << 'ENVEOF'
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1

# S3 Configuration
S3_BUCKET=monokera-bucket
BRONZE_PREFIX=data/bronze
SILVER_PREFIX=data/silver
GOLD_PREFIX=data/gold

# Glue Jobs
GLUE_JOB_CLEAN_DEDUP=spacenews-01-clean-dedup
GLUE_JOB_ENRICH=spacenews-02-enrich-topics-entities
GLUE_JOB_TRENDS=spacenews-03-trends-aggregations

# Redshift
REDSHIFT_DATABASE=dev
REDSHIFT_WORKGROUP=monokera-pipeline
REDSHIFT_IAM_ROLE=arn:aws:iam::311048569989:role/service-role/AmazonRedshift-CommandsAccessRole-2024-01-15T21:53:55.123Z

# Airflow
LOCAL_MODE=false
AIRFLOW_HOME=/opt/airflow

# SNS
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:311048569989:spacenews-pipeline-alerts
ENVEOF
    echo "⚠️  IMPORTANT: Update .env with your AWS credentials!"
else
    echo "✅ .env file already exists"
fi

# Start services
echo "🏃 Starting Docker services..."
COMPOSE_CMD=$(compose_cmd)
if [ -n "$COMPOSE_CMD" ]; then
    $COMPOSE_CMD down --remove-orphans 2>/dev/null || true
    $COMPOSE_CMD up -d || true
else
    echo "⚠️ Docker Compose command not found. Skipping service startup."
fi

# Wait for services
echo "⏳ Waiting 30 seconds for services to initialize..."
sleep 30

# Final checks
echo ""
echo "🏥 Service health checks:"
if [ -n "$COMPOSE_CMD" ]; then
    $COMPOSE_CMD ps || true
fi
echo ""
echo "✅ EC2 setup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Update .env with your AWS credentials"
echo "2. Ensure GitHub has your EC2's SSH public key"
echo "3. Set up GitHub Secrets (see DEPLOYMENT.md)"
echo "4. Monitor logs: docker compose logs -f airflow-scheduler"
