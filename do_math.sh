#!/usr/bin/env bash

set -euo pipefail

# Define colors and styles using ANSI escape codes
GREEN='\033[1;32m'
RED='\033[1;31m'
BOLD='\033[1m'
RESET='\033[0m'

#Let's get the current project name
current_project=$(<.current_project)

printf "\nOk, do you want to generate some predictions for %s ${BOLD}(${GREEN}y${RESET}${BOLD}/${RED}n${RESET}${BOLD})${RESET}?\nWhen that's done, we'll launch the plotting tool.\n\n" "$current_project"


exercise_choice () {
    read -rp "> " choice

    case $choice in 
        y|Y)
            python auto_author_prediction.py;
            python make_auto_scatterplot.py
            ;;
        n|N)
            printf "\nOk. If you want to change the project and try again, just edit .current_project. See you!"
            ;;
        q|quit)
            exit 0
            ;;
        *)
            exercise_choice
            ;;
    esac
}

exercise_choice