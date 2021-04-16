from flask import Flask, jsonify
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# Generates Engine To The Correct sqlite File
engine = create_engine("sqlite:///hawaii.sqlite")

# Reflects The DataBase Schema
Base = automap_base()
Base.prepare(engine, reflect=True)

# Saves References To Tables 
Station = Base.classes.station
Measurement = Base.classes.measurement

# Flask Setup
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
        return (
            f"Welcome to the Climate App!<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/<br/>"
            f"Route Accepts Start Date Parameter and Start/End Date Parameters!"
        )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Creates And Binds The Session Between The Python App And Database 
    session = Session(engine)

    # Session Query
    date_and_prcp = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= '2016-08-23').all()

    # For Loop List Of Tuples
    prcp_rows = [{"Date": result[0], "precipitation": result[1]} for result in date_and_prcp]

    session.close()

    # Returns JSON
    return jsonify(prcp_rows)

@app.route("/api/v1.0/stations")
def stations():
    
    # Creates Session That Names All Stations 
    session = Session(engine)

    stationname = session.query(Station.name).all()

    stationnamerows = [{"Station": result[0]} for result in stationname]

    session.close()

    return jsonify(stationnamerows)

@app.route("/api/v1.0/tobs")
def observations():
    
    session = Session(engine)
    # Dates and Temperature Observations Of The Most Active Station For The Last Year Of Data
    date_and_tobs = session.query(Measurement.station, Measurement.date, Measurement.tobs).\
        filter(Measurement.station == 'USC00519281').\
        filter(Measurement.date >= '2016-08-18').\
        order_by(Measurement.date.desc()).all()

    active_station = [{"Station":result[0],"Date": result[1], "Temperature" :result[2]
                        } for result in date_and_tobs]

    session.close()

    return jsonify(active_station)

@app.route("/api/v1.0/<start>")
def start_date(start):
    
    # Creating Date List
    session = Session(engine)
    date_query = session.query(Measurement.date)

    daterows = [{"Date":result[0]} for result in date_query]
    session.close()

    for date in daterows:
        date_in_list = date["Date"]
        # If Start Date Is A Valid Date 
        if date_in_list == start:
            session = Session(engine)

            calulation = session.query(func.min(Measurement.tobs),
                            func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                         filter(Measurement.date >= start).\
                         order_by(Measurement.date.desc()).all()

            calulationlist = [{"TMIN":result[0], "TAVG":result[1], 
                               "TMAX":result[2]} for result in calulation]
            
            session.close()

            return jsonify(calulationlist)
    
    # Throws Error If Date Was Not Found
    return jsonify({"error": f"Date {start} not found. Please, write date in format YYYY-MM-DD between dates 2010-01-01 to 2017-08-23"}), 404

@app.route("/api/v1.0/<start>/<end>")
def start_and_end_date(start, end):
    
    # Creating Date List
    session = Session(engine)
    date_query = session.query(Measurement.date).\
                 order_by(Measurement.date.asc()).all()

    daterows = [{"Date":result[0]} for result in date_query]
    session.close()
    
    # Setting Variables 
    start_date_boolean = False
    end_date_boolean = False
    startcount = 0 
    endcount = 0 
    
    # Finding Start Date In List
    for date1 in daterows:
        search_start = date1["Date"]
        startcount += 1
        if search_start == start:
            found_start = search_start 
            finalstartcount = startcount
            start_date_boolean = True 
            break
    
    # Checks If Start Date Is A Valid Date
    if start_date_boolean == False:
        return jsonify({"error": f"Start Date: {start} not found. Please, write start date in correct format YYYY-MM-DD between dates 2010-01-01 and 2017-08-23"}), 404
    
    # Finding End Date In List
    for date2 in daterows:
        search_end = date2["Date"]
        endcount += 1
        if search_end == end:
            found_end = search_end 
            finalendcount = endcount
            end_date_boolean = True
            break
    
    # Checks If End Date Is A Valid Date
    if end_date_boolean == False:    
        return jsonify({"error": f"End date: {end} not found. Please, write end date in correct format YYYY-MM-DD and choose end date between dates {start} and 2017-08-23"}), 404
    
    # Making Sure End Date Is After Start Date
    if finalstartcount <= finalendcount:
        if ((start_date_boolean == True) and (end_date_boolean == True)):
            
            # Find Calculations If Dates Are Valid
            session = Session(engine)
            calulation = session.query(func.min(Measurement.tobs),
                            func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                         filter(Measurement.date >= found_start).\
                         filter(Measurement.date <= found_end).\
                         order_by(Measurement.date.desc()).all()
                
            calulationlist = [{"TMIN":result[0], "TAVG":result[1], 
                                "TMAX":result[2]} for result in calulation]    
            session.close()   
            return jsonify(calulationlist)
        
        # Else Statement If It Catches Any Errors
        else:
            jsonify({"error": f"Start date: {start} or/and End date {end} cannot be found. Please, write start and end date in correct format YYYY-MM-DD and where start date is between 2010-01-01 and end date is between start date and 2017-08-23"}), 404
    # Returns If End Date Is Before Start Date
    else:
        return jsonify({"error": f"End date: {end} cannot be a date before Start Date. Please, write end date in correct format YYYY-MM-DD and choose end date between dates {start} and 2017-08-23"}), 404

if __name__ == "__main__":
    app.run(debug=True)