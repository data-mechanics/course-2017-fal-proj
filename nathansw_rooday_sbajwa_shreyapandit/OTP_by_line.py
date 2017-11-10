import json
import pprint 
import datetime
import dml
import prov.mode
import uuid
from bson import ObjectId

### This transformation selects and aggregates data from MBTAPerformance.json
### 
### The keys to the resulting dataset will be all of the MBTA lines (from 
### the bus, rail, and commuter rail), and their corresponding values
### will be average on time performance for both peak periods and off-peak 
### periods 
###
### We will be able to use this new dataset to visualize which MBTA lines
### have the most reliable performance and which have the least reliable 
### performance (based on historical data)


class OTP_by_line(dml.Algorithm):
    contributor = 'nathansw_rooday_sbajwa_shreyapandit'
    ### make sure this is the correct dataset file name
    reads = ['nathansw_rooday_sbajwa_shreyapandit.MBTAPerformance']
    writes = ['nathansw_rooday_sbajwa_shreyapandit.OTP_by_line']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nathansw_rooday_sbajwa_shreyapandit', 'nathansw_rooday_sbajwa_shreyapandit')

        performance_db = repo['nathansw_rooday_sbajwa_shreyapandit.MBTAPerformance']
        perf = performance_db.find_one()

        # create a dict where each key is an MBTA line
        lines = {}
        for date in perf:
        	for entry in perf[date]:
                 route = (perf[date][entry]['ROUTE_OR_LINE'])
                 # if adding a key to dict, add values to hold peak/off-peak data
                 if route not in lines:
                     lines[route] = {'Peak Service': [], 'Off-Peak Service': []}
             
        # aggregate all otp numerator and denominator values for each line     
        for date in perf:
            for entry in perf[date]:
                num = (perf[date][entry]['OTP_NUMERATOR'])
                dem = (perf[date][entry]['OTP_DENOMINATOR'])
                route = (perf[date][entry]['ROUTE_OR_LINE'])
                serv = (perf[date][entry]['PEAK_OFFPEAK_IND'])
                
                # if data was collected during off-peak periods, modify appropriate value
                if 'Off-Peak' in serv:
                    if(len(lines[route]['Off-Peak Service'])) == 0:
                        lines[route]['Off-Peak Service'].append(num)
                        lines[route]['Off-Peak Service'].append(dem)
                    else:
                        lines[route]['Off-Peak Service'][0] += num
                        lines[route]['Off-Peak Service'][1] += dem
                # if data was collected during peak periods, modify appropriate value
                else:
                    if (len(lines[route]['Peak Service'])) == 0:
                        lines[route]['Peak Service'].append(num)
                        lines[route]['Peak Service'].append(dem)                
                    else:
                        lines[route]['Peak Service'][0] += num
                        lines[route]['Peak Service'][1] += dem

        # lines should now be a dictionary holding summed up values of
        # peak OTP numerators and denominators and off-peak OTP numerators
        # and denominators (ex: {'57' {'Peak Service': [1000, 2000], 'Off-Peak Service': [3000, 40000]}})
        avg_OTP = {}
        # calculate OTP percentage
        for rte in lines.keys():
            # create dictionary key and values template for each route 
            avg_OTP[rte] = {'Peak Service': 0.0, 'Off-Peak Service': 0.0}

            # if both peak and off-peak values were collected
            if len(lines[rte]['Off-Peak Service']) != 0 and len(lines[rte]['Peak Service']) != 0:
                peak_perc = float(lines[rte]['Peak Service'][0])/(lines[rte]['Peak Service'][1]) * 100
                avg_OTP[rte]['Peak Service'] = round(peak_perc, 2)
                off_perc = float(lines[rte]['Off-Peak Service'][0])/(lines[rte]['Off-Peak Service'][1]) * 100
                avg_OTP[rte]['Off-Peak Service'] = round(off_perc, 2)

            # if only peak values were calculated
            elif len(lines[rte]['Off-Peak Service']) == 0 and len(lines[rte]['Peak Service']) != 0:
                peak_perc = float(lines[rte]['Peak Service'][0])/(lines[rte]['Peak Service'][1]) * 100
                avg_OTP[rte]['Peak Service'] = round(peak_perc, 2)
                avg_OTP[rte]['Off-Peak Service'] = 'Data not available'

            # if only off-peak values were calculated 
            elif len(lines[rte]['Off-Peak Service']) != 0 and len(lines[rte]['Peak Service']) == 0:
                avg_OTP[rte]['Peak Service'] = 'Data not available'
                off_perc = float(lines[rte]['Off-Peak Service'][0])/(lines[rte]['Off-Peak Service'][1]) * 100
                avg_OTP[rte]['Off-Peak Service'] = round(off_perc,2)
       
        avg_OTP = json.dumps(avg_OTP, indent=4)
        json_performance = json.loads(avg_OTP)

        repo.dropCollection('OTP_by_line')
        repo.createCollection('OTP_by_line')
        repo['nathansw_rooday_sbajwa_shreyapandit.OTP_by_line'].insert_one(json_performance)

        repo.logout()
        
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime=None, endTime=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo

        ### Nathan do ya thang
        

        


        
          