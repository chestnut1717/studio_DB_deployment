from flask import Flask, request, jsonify, Response
from utils.db_connector import DBManagement
import json
from typing import List
from utils.manage_response import *


app = Flask(__name__)


@app.route('/')
def index():
    return "Hello Flask"

@app.route('/db_check', methods=['GET'])
def db_check():
    if request.method == 'GET':


        # requested data from web server
        facilities_type = request.args.get('facilities_type').split(',')
        lat             = float(request.args.get('lat'))
        lon             = float(request.args.get('lon'))
        radius_meter    = int(request.args.get('radius'))
        
        # request to rds
        response_list = request_to_rds(facilities_type, lat, lon, radius_meter)

        total_count   = response_list[0]
        facility_body = response_list[1]
        hashtag_list  = response_list[2]

        # response to web_server
        response_dict = {
                        'status'  : 200,
                        'location': {
                                    'total_count' : total_count,
                                    'facility_type' : facility_body,
                                    'hashtag': hashtag_list
                                    }
                        }

        response = Response(json.dumps(response_dict), mimetype='application/json', status=200)
        
        return response

    else:
        return 'Not GET request', 404

@app.route('/db_check_two')
def db_check_two():
    if request.method == 'GET':
        
        facilities_type = request.args.get('facilities_type').split(',')
        radius_meter = int(request.args.get('radius'))

        #location 1
        lat_1 = float(request.args.get('lat_1'))
        lon_1 = float(request.args.get('lon_1'))
        #location 2
        lat_2 = float(request.args.get('lat_2'))
        lon_2 = float(request.args.get('lon_2'))

        # response for location 1
        response_list_1 = request_to_rds(facilities_type, lat_1, lon_1, radius_meter)
        total_count_1, facility_body_1, hashtag_list_1  = response_list_1[0], response_list_1[1], response_list_1[2]

        # response for location 2
        response_list_2 = request_to_rds(facilities_type, lat_2, lon_2, radius_meter)
        total_count_2, facility_body_2, hashtag_list_2  = response_list_2[0], response_list_2[1], response_list_2[2]

        # scoring
        individual_score_1, total_score_1 = calculate_score(facilities_type, total_count_1, facility_body_1)
        individual_score_2, total_score_2 = calculate_score(facilities_type, total_count_2, facility_body_2)

        
        # response to web server
        response_dict = {
                        'status'  : 200,
                        'location_1': {
                                    'total_count' : total_count_1,
                                    'facility_type' : facility_body_1,
                                    'hashtag': hashtag_list_1,
                                    'score' : {
                                            "total_score": total_score_1,
                                            "individual_score": individual_score_1
                                               }
                                    },
                        'location_2': {
                                    'total_count' : total_count_2,
                                    'facility_type' : facility_body_2,
                                    'hashtag': hashtag_list_2,
                                    'score' : {
                                            "total_score": total_score_2,
                                            "individual_score": individual_score_2
                                               }
                                    },
                        }

        categories = list(individual_score_1.keys())
        values1 = list(individual_score_1.values())
        values2 = list(individual_score_2.values())


        response = Response(json.dumps(response_dict), mimetype='application/json', status=200)
        return response

        
        
    else:
        return 'Not Get request', 404

if __name__ == "__main__":
    app.run(debug=True)


