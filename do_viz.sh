#!/usr/bin/env bash

set -euo pipefail

open_dash () {
    sleep 4;
    open 'http://127.0.0.1:8050/'
}

menu () {
    tput clear;
    printf "\nWhat kind of visualization would you like to make?\n"
    printf "\t1. Interactive Line Dashboard\n"
    printf "\t2. Selective Line Graph\n"
    printf "\t3. Scatterplot\n"
    printf "\t4. Histogram\n"
    printf "\t5. Confusion Matrix\n"
    printf "\tQ. Quit\n"
    read -rp "> " viz_choice
    
    case "$viz_choice" in
        1)
            open_dash &
            python make_dash.py ;;
        2)
            python make_lines.py ;;
        3)
            python make_auto_scatterplot.py ;;
        4)
            python make_histogram.py ;;
        5)
            python make_confusion.py ;;
        q|Q)
            exit 0 ;;
        *)
            menu
    esac
    
}

menu
