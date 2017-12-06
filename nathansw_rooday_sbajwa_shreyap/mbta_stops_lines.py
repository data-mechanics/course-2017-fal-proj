import json
from urllib.request import urlopen
import time
import pprint 
import datetime
import dml
import prov.model
import uuid
from bson import ObjectId

### Algorithm 1

class mbta_stops_lines(dml.Algorithm):

  contributor = 'nathansw_rooday_sbajwa_shreyap'
  ### Make sure this is the correct dataset file name
  reads = ['nathansw_rooday_sbajwa_shreyap.MBTAPerformance', 'nathansw_rooday_sbajwa_shreyap.OTP_by_line']
  # Currently it just creates a csv file 
  writes = []

  @staticmethod
  def execute(trial=False):
    startTime=datetime.datetime.now()
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')
    
    mbta_db = repo['nathansw_rooday_sbajwa_shreyap.OTP_by_line']
    mbta_data = mbta_db.find_one()
    del mbta_data['_id']

    url_base = "http://realtime.mbta.com/developer/api/v2/stopsbyroute?"
    api_key = 'api_key=' + dml.auth['services']['MBTADevelopmentPortal']['key']
    form = "&format=json"

    # Grab all unique route_ids from previously acquired mbta data, set as keys
    # First value for every key - peak OTP %
    route_ids = {}
    cr_lines = ['Franklin', 'Needham', 'Haverhill', 'Lowell', 'Worcester', 'Fitchburg', 'Fairmount', 'Greenbush']
    # Convert route IDs as they appear in performance data to how they need to appear for the MBTA api
    for rte in mbta_data:
      peak = mbta_data[rte]['Peak Service']
      # Blue/Red/Orange Line -> Blue/Red/Orange, Green-X Line -> Green-X
      if 'Blue' in rte or 'Red' in rte or 'Orange' in rte or 'Green' in rte:
        temp = rte.split(' ')
        route_ids[temp[0]] = [peak]
      # Commuter rail lines with two destinations that are only searched under the main destination
      if 'Stroughton' in rte or 'Providence' in rte:
        route_ids['CR-Providence'] = [peak]
      if 'Kingston/Plymouth' in rte:
        route_ids['CR-Kingston'] = [peak]
      if 'Rockport' in rte or 'Newbury' in rte:
        route_ids['CR-Newburyport'] = [peak]
      # Crosstown and silver line buses conversion to number vals
      if rte.startswith("CT") == True:
        if rte == 'CT1':
          route_ids['701'] = [peak]
        if rte == 'CT2':
          route_ids['747'] = [peak]
        if rte == 'CT3':
          route_ids['708'] = [peak]
      if rte.startswith("SL") == True:
        if rte == 'SL4':
          route_ids['751'] = [peak]
        if rte == 'SL5':
          route_ids['749'] = [peak]

      # If it is a bus line, leave as is
      if rte.isdigit() == True:
        route_ids[rte] = [peak]
      # If it is a CR line from the array, modify formatting 
      for cr in cr_lines:
        if cr in rte:
          new_id = 'CR-'+cr
          route_ids[new_id]= [peak]

    # Create a duplicate dictionary to hold parent station info
    # Route_ids currently = {line: [peak OTP], line: ...}
    # Route_ids by the end = {line: [peak OTP, stop_id, stop_id, ...]}
    # Parent_stops by the end = {line: [peak OTP, parent_station_id, parent_station_id, ...]}
    parent_stops = route_ids

    for rte in route_ids:
      # Call MBTA StopsByRoute api for every route id collected
      url = url_base + "&route=" + rte + form
      try:
        temp = json.loads(urlopen(url).read().decode('utf-8'))
      except:
        continue
      for obj in temp['direction']:
        # Iterate over every stop for each route 
        for stop in obj['stop']:
          station = (stop['stop_id'])
          parent = (stop['parent_station'])
          # Add station/parent station ID as a value to the key(route) in the appropriate dictionary
          if station not in route_ids[rte]:
            route_ids[rte].append(station)
          if parent not in parent_stops[rte]:
            if parent == "":
              parent_stops[rte].append('N/A')
            else:
              parent_stops[rte].append(parent)


    ### First csv - column 1 = line, column 2 = line's peak OTP, column 3+ = 1 or 0 if line passes through PARENT stop in column header
    stations = []
    # Get ids of all parent stops (no duplicates)
    for line in parent_stops:
      # Ignore first value for each key
      for stop in parent_stops[line][1:]:
        if stop not in stations and stop != "N/A":
          stations.append(stop)

    # Aggregate name of parent stations into one long, comma separated string
    col_parents = ",".join(stations)
    cols = 'Line,Peak OTP,'+ col_parents + '\n'
    parent_csv = open('lines_vs_parents.csv', 'w')
    parent_csv.write(cols)

    for line in parent_stops:
      peak = parent_stops[line][0]
      # Write name of line and peak value as first two values in each row
      parent_csv.write(line + "," + str(peak) + ",")  
      # Parent stops for the given line
      line_stops = parent_stops[line][1:] 
      line_vals = []
      # Iterate over every parent stop that was collected 
      for stp in stations:  
        # If a route does not pass through one of the parent stops, row value is 0
        if stp not in line_stops: 
          line_vals.append('0')
        else:
          line_vals.append('1')
      # Aggregate row vals (array) into comma separated string
      line_vals = ",".join(line_vals)
      parent_csv.write(line_vals + "\n")

    ### Second csv - column 1 = line, column 2 = peak OTP, column 3+ = 1 or 0 if line passes through stop in column header
    stops = []
    # Get ids of all stops (no duplicates)
    for line in route_ids:
      # Ignore first value for each key
      for stp in route_ids[line][1:]:
        if stp not in stops:
          stops.append(stp)

    # Aggregate ids of stops into comma separated string
    col_stops = ",".join(stops)
    cols = 'Line, Peak OTP,'+col_stops+'\n'

    csv = open('lines_vs_stops.csv', 'w')
    csv.write(cols)

    for line in route_ids:
      peak = route_ids[line][0]
      csv.write(line + "," + str(peak) + ",")
      # Stops for the given line
      line_stops = route_ids[line][1:]  
      line_vals = []
      # Iterate over every stop collected
      for stp in stops: 
        # If route does not pass through stop, row value is 0
        if stp not in line_stops: 
          line_vals.append('0')
        else:
          line_vals.append('1')

      # Aggregate all row vals into comma separated string
      line_vals = ",".join(line_vals)
      csv.write(line_vals + "\n")

    repo.logout()

    endTime = datetime.datetime.now()

    return {"start":startTime, "end":endTime}

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime=None, endTime=None):

    client = dml.pymongo.MongoClient()
    repo = client.repo

    ##########################################################

    ## Namespaces
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nathansw_rooday_sbajwa_shreyap/') # The scripts in / format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap/') # The data sets in / format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

    doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

    ## Agents
    this_script = doc.agent('alg:nathansw_rooday_sbajwa_shreyap#mbta_stops_lines', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ## Activities
    get_lines_vs_parents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    get_lines_vs_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

    ## Entitites
    # Data Source
    resource1 = doc.entity('mbta:stopsbyroute', {'prov:label':'MBTA Stops By Route', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource2 = doc.entity('dat:otp_by_line.json', {'prov:label':'On-Time Performance by Line', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

    # Data Generated
    lines_vs_parents = doc.entity('dat:nathansw_rooday_sbajwa_shreyap#lines_vs_parents', {prov.model.PROV_LABEL:'lines_vs_parents', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'csv'})
    lines_vs_stops = doc.entity('dat:nathansw_rooday_sbajwa_shreyap#lines_vs_stops', {prov.model.PROV_LABEL:'lines_vs_stops', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'csv'})

    ############################################################

    ## wasAssociatedWith
    doc.wasAssociatedWith(get_lines_vs_parents, this_script)
    doc.wasAssociatedWith(get_lines_vs_stops, this_script)

    ## used   
    doc.usage(get_lines_vs_parents, resource1, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})
    doc.usage(get_lines_vs_stops, resource1, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})

    doc.usage(get_lines_vs_parents, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})
    doc.usage(get_lines_vs_stops, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})

    ## wasGeneratedBy
    doc.wasGeneratedBy(lines_vs_parents, get_lines_vs_parents, endTime)
    doc.wasGeneratedBy(lines_vs_stops, get_lines_vs_stops, endTime)

    ## wasAttributedTo    
    doc.wasAttributedTo(lines_vs_parents, this_script)
    doc.wasAttributedTo(lines_vs_stops, this_script)

    ## wasDerivedFrom
    doc.wasDerivedFrom(lines_vs_parents, resource1, get_lines_vs_parents, get_lines_vs_parents, get_lines_vs_parents)
    doc.wasDerivedFrom(lines_vs_stops, resource1, get_lines_vs_stops, get_lines_vs_stops, get_lines_vs_stops)   

    doc.wasDerivedFrom(lines_vs_parents, resource2, get_lines_vs_parents, get_lines_vs_parents, get_lines_vs_parents)
    doc.wasDerivedFrom(lines_vs_stops, resource2, get_lines_vs_stops, get_lines_vs_stops, get_lines_vs_stops)

    ############################################################

    repo.logout()

    return doc