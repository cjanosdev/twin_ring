#!/bin/bash
set -e
cd "$(dirname "$0")"

# Install if needed
if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm install
fi

echo "Starting dashboard..."
echo "  Backend → http://localhost:8080"
echo "  Frontend → http://localhost:5173"
npm run dev
