from flask import Flask
from flask import request
from sklearn.externals import joblib
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler  


app = Flask(__name__)

@app.route('/ml')
def ml():

    scaler = joblib.load('scaler.save')
    model = joblib.load('mlmodel.sav')
    
    CarbonIntensity = request.args.get('ci', '')
    EnergyIntensity = request.args.get('ei', '')
    EnergyUse = request.args.get('eu', '')
    GDPperCapita = request.args.get('gdp', '')
    HDI = request.args.get('hdi', '')
    Population = request.args.get('pop', '')
    inputarr=[]
    tmp=[]

    tmp.append(CarbonIntensity)
    tmp.append(EnergyIntensity)
    tmp.append(EnergyUse)
    tmp.append(GDPperCapita)
    tmp.append(HDI)
    tmp.append(Population)
    inputarr.append(tmp)


    inputs = scaler.transform(inputarr)
    pred = model.predict(inputs)
    returnval = float(pred[0])
    return str(returnval)
    

 
@app.route('/stats')
def stats():

    
    model = joblib.load('statsmodel.sav')
    
    CarbonIntensity = float(request.args.get('ci', ''))
    EnergyIntensity = float(request.args.get('ei', ''))
    EnergyUse = float(request.args.get('eu', ''))
    GDPperCapita = float(request.args.get('gdp', ''))
    HDI = float(request.args.get('hdi', ''))
    Population = float(request.args.get('pop', ''))
    inputarr=[]
    tmp=[]

    tmp.append(CarbonIntensity)
    tmp.append(EnergyIntensity)
    tmp.append(EnergyUse)
    tmp.append(GDPperCapita)
    tmp.append(HDI)
    tmp.append(Population)
    inputarr.append(tmp)

    pred = model.predict(tmp)
    returnval = float(pred[0])
    return str(returnval)

