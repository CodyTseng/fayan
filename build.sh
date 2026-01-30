#!/bin/bash
set -e

echo "Building web frontend..."
cd web
npm install
npm run build
cd ..

echo "Copying static files..."
rm -rf cmd/api/static/*
cp -r web/dist/* cmd/api/static/

echo "Building Go binaries..."
go build -o fayan-crawler ./cmd/crawler/main.go
go build -o fayan-api ./cmd/api/main.go

echo "Build complete!"
