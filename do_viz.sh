#!/usr/bin/env bash

set -euo pipefail

start_dash () {
    python make_dash.py &
    DASH_PID=$!
    printf "Dash app started with PID: %s\nThe dashboard will open shortly...\n" $DASH_PID
}

stop_dash () {
    if [ -n "$DASH_PID" ]; then
        echo "Stopping Dash app (PID: $DASH_PID)..."
        kill "$DASH_PID"
        echo "Dash app stopped."
    else
        echo "Dash app is not running."
    fi
}

open_dash () {
    sleep 4;
    open 'http://127.0.0.1:8050/' &
    wait
}

trap stop_dash INT  # Trap Ctrl+C (SIGINT) and run stop_dash function

menu () {
    tput clear;
    printf "\nWhat kind of visualization would you like to make?\n"
    printf "\t1. Interactive Line Dashboard\n"
    printf "\t2. All vs. All Dashboard (slow)\n"
    printf "\t3. Selective Line Graph\n"
    printf "\t4. Scatterplot\n"
    printf "\t5. Histogram\n"
    printf "\t6. Confusion Matrix\n"
    printf "\tQ. Quit\n"
    read -rp "> " viz_choice

    case "$viz_choice" in
        1)
            start_dash;
            open_dash ;;
        2)
            python make_jumbo_dash.py ;;
        3)
            python make_lines.py ;;
        4)
            python make_auto_scatterplot.py ;;
        5)
            python make_histogram.py ;;
        6)
            python make_confusion.py ;;
        q|Q)
            exit 0 ;;
        *)
            menu
    esac

}

menu
