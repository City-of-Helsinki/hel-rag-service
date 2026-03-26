#!/bin/bash

# Setup script for the decision pipeline backend

set -e

echo "================================================"
echo "Helsinki Decision Pipeline - Setup Script"
echo "================================================"
echo

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Found Python $python_version"

# Check if version is 3.11 or higher
major=$(echo $python_version | cut -d. -f1)
minor=$(echo $python_version | cut -d. -f2)

if [ "$major" -lt 3 ] || ([ "$major" -eq 3 ] && [ "$minor" -lt 11 ]); then
    echo "Error: Python 3.11 or higher is required"
    exit 1
fi

# Create virtual environment
echo
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo
echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env file if it doesn't exist
echo
echo "Setting up environment file..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "✓ Created .env file from .env.example"
    echo "  You can edit .env to customize settings"
else
    echo "✓ .env file already exists"
fi

# Create data directories
echo
echo "Creating data directories..."
mkdir -p data/decisions
mkdir -p logs
echo "✓ Directories created"

# Run tests
echo
echo "Running tests..."
if pytest tests/ -v; then
    echo "✓ All tests passed"
else
    echo "⚠ Some tests failed, but setup is complete"
fi

echo
echo "================================================"
echo "Setup Complete!"
echo "================================================"
echo
echo "Next steps:"
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo
echo "2. Review and customize settings in .env (optional)"
echo
echo "3. Start fetching data:"
echo "   python pipeline.py fetch"
echo
echo "4. View statistics:"
echo "   python pipeline.py stats"
echo
echo "For more information, see README.md"
echo
