import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from database_ops import export_results_to_csv
from util import get_project_name

matplotlib.use('agg') #Alternate backend to avoid memory leak.

project_name = get_project_name()

def plot_things():
    plt.figure(figsize=(25, 25))
    ax=plt.subplot(111)
    plt.rcParams["axes.labelsize"] = 30
    plt.rcParams["ytick.labelsize"] = 25

    columns = ["EarlierComp", "LaterComp", "Ngrams", "Hapaxes", "Alignments"]
    df = pd.read_csv(f"./projects/{project_name}/results/results.csv", names=columns, low_memory=False)
    sns.scatterplot(data=df, x="Hapaxes", y="Ngrams", hue="Alignments", s=300)

    for i in range(df.shape[0]):
        earlier = str(df.EarlierComp[i])
        later = str(df.LaterComp[i])
        label = earlier + " - " + later
        plt.text(x=df.Hapaxes[i] + 0.3, y=df.Ngrams[i] + 0.3, s=label)

    ax.tick_params(labelsize='19', width=3)
    ax.tick_params(axis='x', which='minor', labelsize=9, width=3)
    plt.yticks(rotation=0)
    plt.xticks(rotation=90)

    plt.savefig('projects' + '/' + project_name + '/' + 'visualizations' + '/' +'ngrams-hapaxes'+ '.png')

export_results_to_csv()
plot_things()
