# Does what it says on the box; creates a graph for the two authors 
# that have just been analyzed. Stepwise for various thresholds.

import os

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
from sklearn.metrics import precision_recall_curve

from predict_ops import get_confusion_scores


def make_plot(df):
    # Plotting precision and recall as continuous lines
    plt.plot(df.index, df['precision'], "b--", label="Precision")
    plt.plot(df.index, df['recall'], "g-", label="Recall")
    
    # Set plot labels and legend
    plt.xlabel('Threshold')
    plt.ylabel('Score')
    plt.legend()
    
    # Display the plot
    plt.show()

def main():
    confusion_scores_dict = get_confusion_scores()
    
    df = pd.DataFrame.from_dict(confusion_scores_dict).transpose()
    # Add column labels
    column_labels = ['tp', 'tn', 'fp', 'fn']
    df.columns = column_labels
    df['precision'] = df['tp'] / (df['tp'] + df['fp'])
    df['recall'] = df['tp'] / (df['tp'] + df['fn'])
    print("\n")
    print(df)
    make_plot(df)

if __name__ == "__main__":
    os.system('clear')
    main()
