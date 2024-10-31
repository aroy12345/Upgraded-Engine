#!/bin/bash

# Clean up previous run
rm -rf classes/*

# Compile all other Java sources
javac --source-path src -d classes $(find src -name '*.java')

# Open a new terminal window and run the frontend
echo "cd \"$(pwd)\"; java -cp classes frontend.routes" > frontend.sh
chmod +x frontend.sh
open -a Terminal frontend.sh
