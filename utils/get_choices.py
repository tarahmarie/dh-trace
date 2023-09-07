from dataclasses import dataclass

@dataclass
class Choices:
    author_num: int
    threshold: str
    min_length: int

def get_choices_for_viz(author_set, threshold_set):
    auth_choice = None
    while True:
        user_input = input("Select an author number to be the basis of comparison: ")
        
        try:
            auth_choice = int(user_input)
            if auth_choice in author_set.keys():
                break
            else:
                print("Invalid author number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a valid author number or '.' to finish.")

    print(f"\nThese are the available thresholds: {threshold_set}\n")
    chosen_threshold = 0.0
    while True:
        chosen_threshold = input("What threshold do you want to set for the query? ")

        try:
            if (float(chosen_threshold) or int(chosen_threshold)) in threshold_set:
                break
        except ValueError:
            print("Invalid input. Please try again...")

    chosen_min_length = 0
    while True:
        chosen_min_length = input("What is the minimum length (in words) for the texts you want to use? ")

        try:
            if (int(chosen_min_length) > 0):
                break
        except ValueError:
            print("Please pick a number greater than 0.")

    choices = Choices(auth_choice, chosen_threshold, chosen_min_length)
    return choices
