import pandas as pd
import numpy as np
import collections
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Note: all stats obtained from https://www.census.gov/quickfacts/table/PST045216/00
def plot_breakdown():
    '''Plots the basic race breakdown in Boston, MA as a pie chart'''
    race_stats = [53.9, 24.4, 17.5, 8.9]
    labels = ['White', 'Black', 'Hispanic', 'Asian']
    colors =   colors = ["red", 'lightskyblue', "green", "purple", 'blue', 'gold', 'lightcoral']

    fig, ax = plt.subplots()
    plt.title('Breakdown of Boston Citizens by Race in 2010')
    ax.pie(race_stats, labels=labels, colors = colors, autopct='%1.2f%%', startangle=60)
    ax.axis('equal')

    plt.savefig('race_chart.png')



def district_pie_chart(percentages, district):
    labels = ["Black", "White", "Hispanic", "Asian/Pacific Islander", "Native American", "Other", "Multiracial"]
    
    fig, ax = plt.subplots()
    colors = ["red", 'lightskyblue', "green", "purple", 'blue', 'gold', 'lightcoral']
    patches = ax.pie(percentages, colors = colors, autopct='%1.2f%%', startangle=60, pctdistance = 1.2)
    plt.legend(patches[0], labels, bbox_to_anchor=(1.35, 1))
    ax.axis('equal')
    if(district.lower() == "south end"):
        plt.title("Race Percentages in " + district[0].upper() + district[1:] + " in 2010", y = 1.1)
    else:
        plt.title("Race Percentages in " + district[0].upper() + district[1:] + " in 2010")
    plt.tight_layout()

    plt.savefig('race_breakdown.png')


plot_breakdown()
roxbury = [47.9, 20.4, 20.3, 2.7, 0.4, 3.7, 4.6]
south_end = [7.9, 65.6, 9.5, 0.3, 14.6, 0.4, 1.7]
district_pie_chart(roxbury, "Roxbury")
district_pie_chart(south_end, "South End")
