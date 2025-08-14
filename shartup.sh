#!/bin/bash
echo "Installing Playwright Browsers..."

# Use a writable path during GitHub Actions deployment
export PLAYWRIGHT_BROWSERS_PATH=/home/runner/.playwright-browsers
mkdir -p $PLAYWRIGHT_BROWSERS_PATH

# Ensure playwright is installed
python -m pip install --upgrade pip
python -m pip install --upgrade playwright

# Install Playwright browsers
playwright install --with-deps

echo "Playwright Browsers Installed"
