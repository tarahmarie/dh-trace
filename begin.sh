#!/usr/bin/env bash

set -euo pipefail

#Let's make a list of projects
PROJECTS=()
while IFS='' read -r line; do PROJECTS+=("$line"); done < <(basename -a ./projects/*/ | paste -d '\n' -s -)

#Some helper variables
project_file_count=""
last_run_file_count=""

#Kicks off at the start. Sets up a new project, or moves you on to picking an existing project.
#A project targets batches of texts, of any kind. This block asks to be pointed at the /splits dir 
#to find texts in the format it needs, and looks for the relevant inputs like alignments.

initialize_new_project () {
    tput clear;
    printf "\n\tHello!\n\n\t"

    read -rp "Would you like to prepare the folders for a new project: (y/n) " new_project_choice
    
    local lower_choice
    lower_choice=$(echo "$new_project_choice" | awk '{print tolower($0)}')

    if [ "$lower_choice" == "y" ]; then
        printf "\t"
        read -rp "What should I name the project? " new_project_name
        printf "\t"
        read -rp "Is $new_project_name correct? (y/n) " confirm_new_project_name
        
        local confirm_new_project_name_choice
        confirm_new_project_name_choice=$(echo "$confirm_new_project_name" | awk '{print tolower($0)}')

        if [ "$confirm_new_project_name_choice" == "y" ]; then
            printf "\n\tOK, making project..."
            mkdir -p ./projects/"$new_project_name"/{db,alignments,splits,results,visualizations}
            printf "\n\n\tDirectories created."
            printf "\n\n\tAdd your alignments to ./projects/%s/alignments/" "$new_project_name"
            printf "\n\tAdd your split files to ./projects/%s/splits/" "$new_project_name"
            printf "\n\n"
            exit 0
        elif [ "$confirm_new_project_name_choice" == "n" ]; then
            printf "\n\tOK, quitting..."
            exit 0
        else
            initialize_new_project
        fi        

    elif [ "$lower_choice" == "n" ]; then
        choose_project
    else
        initialize_new_project
    fi
}

#Presents you a list of existing projects and gets your selection for what to work with.
choose_project () {
    tput clear;
    printf "\n\nHere are the existing projects you can work on:\n\n"

    printf '\t%s\n' "${PROJECTS[@]}"

    while true; do
        printf "\n"
        read -rp "Which project would you like to work on?  Just type the name (or 'quit' to exit): " project_name

        if [ "$project_name" == "quit" ]; then
            printf "\nOK, quitting...\n"
            exit 0
        elif [[ "${PROJECTS[*]}" =~ ${project_name} ]]; then
            echo "$project_name" > .current_project
            break
        else
            echo "Please choose a project name or 'quit'."
            choose_project
        fi
    done

    check_file_counts
}

#Compares file counts with last run.  If match, asks if you want to re-do.
#If there's not a match (e.g. if you've included an extra piece of text or there's
#an alignment file missing, etc), it does not ask you anything, but continues
#to run the work script with no further prompt. Intended to save you from having to 
#rerun the statistics each time if no changes to the texts have occurred, and 
#will display the previous stats generated.

check_file_counts () {
    project_file_count=$(find ./projects/"$project_name"/splits -type f ! -name '.DS_Store' | wc -l | awk '{print $1}')
    
    if [ -f "projects/$project_name/db/$project_name.db" ]; then
        last_run_file_count=$(sqlite3 -readonly -batch -line ./projects/"$project_name"/db/"$project_name".db "SELECT number_files FROM last_run;" | awk '{print $3}')
    fi

    #Let's see if we need to do anything:
    printf "\n\tNumber of files in project: %s" "$project_file_count"
    printf "\n\tNumber of files in last run: %s" "$last_run_file_count"

    if [ "$project_file_count" == "$last_run_file_count" ]; then
        printf "\n\n"
        read -rp "File count matches from last run. Did you want to run everything again? (y/n) " run_again_choice
        local lower_choice
        lower_choice=$(echo "$run_again_choice" | awk '{print tolower($0)}')
        if [ "$lower_choice" == "y" ]; then
            do_the_work
        elif [ "$lower_choice" == "n" ]; then
            tput clear;
            printf "\n\nOK, here are the stats from the previous run..."
            python show_previous_averages.py;
        else
            check_file_counts
        fi
    else
        do_the_work
    fi
}

#Actually executes the work on a given project. From previous function, doing this
#only if either asked to or if the file count doesn't match from a previous run.
#Deletes old databases, runs the set of functions on files for calculation of
#results and statistics, and stores in fresh dbs.

do_the_work () {
    printf "\n\n\tRemoving old dbs (if they exist) and loading data..."
    printf "\n"

    if [ -f "projects/$project_name/db/sqlite3.db" ]; then
        rm projects/"$project_name"/db/sqlite3.db;
    fi
    if [ -f "projects/$project_name/db/$project_name".db ]; then
        rm projects/"$project_name"/db/"$project_name".db;
    fi
    if [ -f "projects/$project_name/db/$project_name"-predictions.db ]; then
        rm projects/"$project_name"/db/"$project_name"-predictions.db;
    fi

    python load_authors_and_texts.py; # go find all the relevant texts & pair them up.
    python load_alignments.py; 
    python load_ngrams.py;
    python load_hapaxes.py;
    python load_hapax_intersects.py;
    python load_ngram_intersects.py;
    python load_relationships.py;
    python load_jaccard.py;
    #printf "\n\nTo do author prediction, run \`python author_prediction.py\`\n"
    printf "To do batch author prediction (spoiler: slower), run \`do_math.sh\`\n\n"
}

#Check if we're starting a new project.
initialize_new_project
