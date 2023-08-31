#!/usr/bin/env bash

set -euo pipefail

menu () {
    tput clear;
    printf "\nWhat kind of visualization would you like to make?\n"
    printf "\t1. Scatterplot\n"
    printf "\t2. Histogram\n"
    printf "\t3. Line Graph\n"
    printf "\t4. Selective Line Graph\n"
    printf "\t5. Confusion Matrix\n"
    printf "\tQ. Quit\n"
    read -rp "> " viz_choice
    
    case "$viz_choice" in
        1)
            python make_auto_scatterplot.py ;;
        2)
            python make_histogram.py ;;
        3)
            python make_lines.py ;;
        4)
            python make_meta_lines.py ;;
        5)
            python make_confusion.py ;;
        q|Q)
            exit 0 ;;
        *)
            menu
    esac
    
}

menu