import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import datetime
import math
import dml

def get_total_counts(fio_df, crime_df):

    return len(fio_df), len(crime_df)

def add_day_col_and_year_col(df):
    '''Adds day of week column to the original fio df based on the date column.'''

    date_col = df["FIO_DATE"]
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dates = []
    years = []
    for date in date_col:
        d = date.split(" ")[0].split("/")
        wday = datetime.datetime(int(d[2]), int(d[0]), int(d[1])).weekday()
        dates +=[weekdays[wday]]
        years += [int(d[2])]
    df["DAY_OF_WEEK"] = dates
    df["YEAR"] = years
    return df

def add_year_col_crime(df):
    '''Takes the crime df and adds a year column.'''
    date_col = df['OCCURRED_ON_DATE']
    years = []
    for date in date_col:
        years.append(int(date[:4]))

    df["Year"] = years
    return df
    
def plot_fio_and_crime_districts(f_df,c_df, fio_attrib, crime_attrib, name, inclusion = None, exclusion = None, 
                                 reorder_func = None, xlabels = None, 
                                 label_rotation = None):
    

    filtered_f_df = f_df.loc[f_df["YEAR"].isin([2012, 2013, 2014, 2015])]
    filtered_c_df = c_df.loc[c_df["Year"].isin(["2012", "2013", "2014", "2015"])]
  
    fio_attr_groups = f_df.groupby(fio_attrib)
    crime_attr_groups = c_df.groupby(crime_attrib)
    fio_attribs, fio_counts, crime_attribs, crime_counts = [], [], [], []
         
    #obtain desired attributes and their respective counts for fio_data
    for fio_group in fio_attr_groups:
        if(inclusion is not None and fio_group[0] in inclusion):
            fio_attribs += [fio_group[0]]
            fio_counts += [len(fio_group[1])]
        elif(exclusion is not None and fio_group[0] not in exclusion):
            fio_attribs += [fio_group[0]]
            fio_counts += [len(fio_group[1])]
        elif (exclusion is None and inclusion is None):
            fio_attribs += [fio_group[0]]
            fio_counts += [len(fio_group[1])]
    
    #obtain desire attributes and their respective counts for crime_data
    for crime_group in crime_attr_groups:
        if(inclusion is not None and crime_group[0] in inclusion):
            crime_attribs += [crime_group[0]]
            crime_counts += [len(crime_group[1])]
        elif(exclusion is not None and crime_group[0] not in exclusion):
            crime_attribs += [crime_group[0]]
            crime_counts += [len(crime_group[1])]
        elif (exclusion is None and inclusion is None):
            crime_attribs += [crime_group[0]]
            crime_counts += [len(crime_group[1])]
      
    

    if(reorder_func is not None):
        fio_attribs, fio_counts = reorder_func(fio_attribs, fio_counts)
        crime_attribs, crime_counts = reorder_func(crime_attribs, crime_counts)  
        
    
    total_f_count, total_c_count = get_total_counts(fio_df, crime_df)
    
    norm_fio_counts, norm_crime_counts = [], []
    for f_count, c_count in zip(fio_counts, crime_counts):
        norm_fio_counts += [f_count/total_f_count]
        norm_crime_counts += [c_count/total_c_count]
        
    
#    print("FIO_" + name + ": ", fio_attribs)
#    print("FIO Counts based on " + name + ": ", norm_fio_counts)
#    print()
#    print("Crime_" + name + ": ", crime_attribs)
#    print("Crime Counts based on " + name + ": ", norm_crime_counts)
    
    #plot bar graph
    fig, ax = plt.subplots()
    rects1 = plt.bar(np.arange(len(fio_attribs))-0.1, norm_fio_counts, align = "center", color = "r", width = 0.4, label = "FIO Incidents")
    rects2 = plt.bar(np.arange(len(crime_attribs))+0.3, norm_crime_counts, align = "center", width = 0.4, label = "Crimes")
        
    plt.title("Percentage of Crimes and FIO Incidents for Each " + name)
    plt.ylabel("Percentage of Occurences")        
    plt.xlabel(name)
    
    if(xlabels is not None):
        plt.xticks(np.arange(len(xlabels)), xlabels, rotation = label_rotation)
    else:
        plt.xticks(np.arange(len(fio_attribs)), fio_attribs, rotation = label_rotation)
    
    plt.savefig('crime_fios.png')



def order_districts(districts, counts):
    ''' Orders the districts in a more readable way before plotting.'''

    if(counts is not None):
        districts = [districts[0] + " & " + districts[1]] + districts[2:5] + [districts[6]] + [districts[5]] + [districts[8]] + [districts[7]] + [districts[11]] + [districts[9]] + [districts[10]]
        counts = [counts[0] + counts[1]] + counts[2:5] + [counts[6]] + [counts[5]] + [counts[8]] + [counts[7]] + [counts[11]] + [counts[9]] + [counts[10]]
        return districts, counts
    else:
        districts = [districts[0] + " & " + districts[1]] +districts[2:5] + [districts[6]] + [districts[5]] + [districts[8]] + [districts[7]] + [districts[11]] + [districts[9]] + [districts[10]]
        return districts


district_names = ["Downtown and Charlestown", "East Boston", "Roxbury", "Mattapan", "Back Bay/South Boston", "Dorchestor", 
                      "South End", "Brighton", "West Roxbury", "Jamaica Plain", "Hyde Park"]



client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('esaracin', 'esaracin')

dataset = repo['esaracin.crime_incidents'].find()
crime_df = pd.DataFrame(list(dataset))
crime_df = add_year_col_crime(crime_df)

dataset = repo['esaracin.fio_data'].find()
fio_df = pd.DataFrame(list(dataset))
fio_df = add_day_col_and_year_col(fio_df)

plot_fio_and_crime_districts(fio_df, crime_df, "DIST", "DISTRICT", "District", inclusion = "A1A15A7B2B3C6C11D4D14E5E13E18", xlabels = district_names, label_rotation = 80,
                            reorder_func = order_districts)
