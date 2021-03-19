#!/usr/bin/python3
import mysql.connector
import json
import sys
import os
from os import path
from datetime import datetime

def get_values_config():
    with open('/json/database_config.json') as f: ###------------------> This path is where the config file needs to be located
        data = json.load(f)
    path = data["path"]
    #verify if the path exists, if not just add the folder of the DB
    if os.path.exists(path) is False:
        path = ""
    if 'path' in data: 
        del data['path']
    return data, path
    
def datatime__func():
    # Textual month, day and year
    today = datetime.now()
    timestamp = today.strftime("%B %d, %Y %H:%M:%S")
    return timestamp

def vc_master__func(vc_master__file_name, cursor):
    global vc_master__all
    query = ("""SELECT * FROM vc_master WHERE filename = %s""")
    cursor.execute(query,vc_master__file_name)
    vc_master__rows = cursor.rowcount
    print("vc_master_rows -->", vc_master__rows)
    #verify if the query get results or not
    if vc_master__rows == 0:
        sys.exit()
    else :
        vc_master__all = cursor.fetchone()
        vc_master__id_file = [vc_master__all['id_file']]
        vc_rpt_parse_stats__func(vc_master__id_file, cursor)
        vc_rpt_rvd__func(vc_master__id_file, cursor)
        vc_rpt_dtc__func(vc_master__id_file, cursor) ##Added 2nd time
        vc_rpt_event__func(vc_master__id_file, cursor) ## 3

def vc_rpt_parse_stats__func(vc_master__id_file, cursor):
    global vc_rpt_parse_stats__all
    query = ("""SELECT * FROM vc_rpt_parse_stats WHERE id_file = %s""")
    cursor.execute(query,vc_master__id_file)
    vc_rpt_parse_stats__rows = cursor.rowcount
    print("vc_rpt_parse_stats__rows -->", vc_rpt_parse_stats__rows)
    #verify if the query get results or not
    if vc_rpt_parse_stats__rows == 0:
        vc_rpt_parse_stats__all = {'parse_version':None}
    else :
        vc_rpt_parse_stats__all = cursor.fetchone()
    #vc_rpt_parse_stats__all['parse_version'] = '1.150.14' # This is for testing purpose only

def vc_rpt_rvd__func(vc_master__id_file, cursor):
    global vc_rpt_rvd__all
    query = ("""SELECT * FROM vc_rpt_rvd WHERE id_file = %s""")
    cursor.execute(query,vc_master__id_file)
    vc_rpt_rvd__rows = cursor.rowcount
    print("vc_rpt_rvd__rows -->", vc_rpt_rvd__rows)
    #verify if the query get results or not
    if vc_rpt_rvd__rows == 0:
        vc_rpt_rvd__all = {}
        vc_rpt_rvd__id_attach = [0]
        vc_rpt_rvd__id_rvd = [0]
        vc_rpt_attachment(vc_rpt_rvd__id_attach, cursor)
        vc_rpt_rvd_gps(vc_rpt_rvd__id_rvd, cursor)
        vc_rpt_rvd_inuse(vc_rpt_rvd__id_rvd, cursor)
    else :
        vc_rpt_rvd__all = cursor.fetchone()
        vc_rpt_rvd__id_rvd= [vc_rpt_rvd__all['id_rvd']]
        vc_rpt_rvd__id_attach = [vc_rpt_rvd__all['id_attach']]
        vc_rpt_attachment(vc_rpt_rvd__id_attach, cursor)
        vc_rpt_rvd_gps(vc_rpt_rvd__id_rvd, cursor)
        vc_rpt_rvd_inuse(vc_rpt_rvd__id_rvd, cursor)

def vc_rpt_attachment(vc_rpt_rvd__id_attach, cursor):
    global vc_rpt_attachment__all
    query = ("""select * from vc_rpt_attachment where id_attach = %s""")
    cursor.execute(query,vc_rpt_rvd__id_attach)
    vc_rpt_attachment__rows = cursor.rowcount
    print("vc_rpt_attachment__rows -->", vc_rpt_attachment__rows)
    #verify if the query get results or not
    if vc_rpt_attachment__rows == 0 :
        vc_rpt_attachment__all = {}
    else :
        vc_rpt_attachment__all = cursor.fetchone()

def vc_rpt_rvd_gps(vc_rpt_rvd__id_rvd, cursor):
    global vc_rpt_rvd_gps__all
    query = ("""select * from vc_rpt_rvd_gps where id_rvd = %s order by timestamp asc""")
    cursor.execute(query,vc_rpt_rvd__id_rvd)
    vc_rpt_rvd_gps__rows = cursor.rowcount
    print("vc_rpt_rvd_gps__rows -->", vc_rpt_rvd_gps__rows)
    #verify if the query get results or not
    if vc_rpt_rvd_gps__rows == 0 :
        vc_rpt_rvd_gps__all = {}
    else :
        vc_rpt_rvd_gps__all = cursor.fetchall()

def vc_rpt_rvd_inuse(vc_rpt_rvd__id_rvd, cursor):
    global vc_rpt_rvd_inuse__all
    query = ("""select * from vc_rpt_rvd_inuse where id_rvd = %s""")
    cursor.execute(query,vc_rpt_rvd__id_rvd)
    vc_rpt_rvd_inuse__rows = cursor.rowcount
    print("vc_rpt_rvd_inuse__rows -->", vc_rpt_rvd_inuse__rows)
    #verify if the query get results or not
    if vc_rpt_rvd_inuse__rows == 0 :
        vc_rpt_rvd_inuse__all = {}
    else :
        vc_rpt_rvd_inuse__all = cursor.fetchone()

def vc_rpt_dtc__func(vc_master__id_file, cursor): ### 2
    global vc_rpt_dtc__all, vc_rpt_dtc_params__all, vc_rpt_attachment__all
    vc_rpt_dtc_params__all = {}
    #vc_rpt_attachment__all = {}
    query = ("""select * from vc_rpt_dtc where id_file = %s""")
    cursor.execute(query,vc_master__id_file)
    vc_rpt_dtc__rows = cursor.rowcount
    print("vc_rpt_dtc__rows -->", vc_rpt_dtc__rows)
    #verify if the query get results or not
    if vc_rpt_dtc__rows == 0 :
        vc_rpt_dtc__all = {}
    else :
        vc_rpt_dtc__all = cursor.fetchall()
        for row in vc_rpt_dtc__all:
            vc_rpt_dtc_params__all.update({row['id_row'] : vc_rpt_dtc_params__func(row['id_row'], cursor)})
            vc_rpt_attachment__all.update({row['id_attach'] : vc_rpt_attachment__func(row['id_attach'], cursor)})

def vc_rpt_dtc_params__func(id_row, cursor): ### 2
    query = ("select * from vc_rpt_dtc_params where id_row ='%s' " % id_row)
    cursor.execute(query)
    vc_rpt_dtc_params__rows = cursor.rowcount
    print("vc_rpt_dtc_params -->", vc_rpt_dtc_params__rows)
    #verify if the query get results or not
    if vc_rpt_dtc_params__rows == 0 :
        vc_rpt_dtc_params__all = {}
    else :
        vc_rpt_dtc_params__all = cursor.fetchall()
    return vc_rpt_dtc_params__all

def vc_rpt_attachment__func(id_attach, cursor): ### 2
    query = ("select * from vc_rpt_attachment where id_attach ='%s' " % id_attach)
    cursor.execute(query)
    vc_rpt_attachment__rows = cursor.rowcount
    print("vc_rpt_attachment -->", vc_rpt_attachment__rows)
    #verify if the query get results or not
    if vc_rpt_attachment__rows == 0 :
        vc_rpt_attachment__all = {}
    else :
        vc_rpt_attachment__all = cursor.fetchall()
    return vc_rpt_attachment__all

def vc_rpt_event__func(id_file, cursor): ### 3
    global vc_rpt_event__all, vc_rpt_event_params__all
    vc_rpt_event_params__all = {}
    #id_file = [1268182] # This is for testing purpose only
    query = ("""select * from vc_rpt_event where id_file = %s""")
    cursor.execute(query,id_file)
    vc_rpt_event__rows = cursor.rowcount
    print("vc_rpt_event -->", vc_rpt_event__rows)
    #verify if the query get results or not
    if vc_rpt_event__rows == 0 :
        vc_rpt_event__all = {}
    else :
        vc_rpt_event__all = cursor.fetchall()
        for row in vc_rpt_event__all:
            vc_rpt_event_params__all.update({row['id_row'] : vc_rpt_event_params__func(row['id_row'], cursor)})
            vc_rpt_attachment__all.update({row['id_attach'] : vc_rpt_attachment__func(row['id_attach'], cursor)})

def vc_rpt_event_params__func(id_row, cursor): ### 3
    query = ("select * from vc_rpt_event_params where id_row ='%s' " % id_row)
    cursor.execute(query)
    vc_rpt_event_params__rows = cursor.rowcount
    print("vc_rpt_event_params -->", vc_rpt_event_params__rows)
    #verify if the query get results or not
    if vc_rpt_event_params__rows == 0 :
        vc_rpt_event_params__all = {}
    else :
        vc_rpt_event_params__all = cursor.fetchall()
    return vc_rpt_event_params__all

def define_structure():
    #join the data, in order to get a specific result
    data = {'results':{'metadata':{'parse_version':vc_rpt_parse_stats__all['parse_version'], 
                'gen_timestamp':datatime__func()},'vc_master':vc_master__all, 'vc_rpt_RVD':vc_rpt_rvd__all, 
                'vc_rpt_rvd_gps':vc_rpt_rvd_gps__all,'vc_rpt_rvd_inuse':vc_rpt_rvd_inuse__all,'vc_rpt_DTC':vc_rpt_dtc__all,'vc_rpt_dtc_params':vc_rpt_dtc_params__all,
                'vc_rpt_attachment':vc_rpt_attachment__all,'vc_rpt_EVENT':vc_rpt_event__all,'vc_rpt_event_params':vc_rpt_event_params__all
                }
            }
    return data

def create_json(data_to_json, path_json_result):
    dir_filename = str(vc_master__all['filename']).split("_")
    dir_filename = "_".join(dir_filename[:-2]) #This removes the last two values on the list then join them again

    if not path_json_result:
        script_path = os.getcwd() ###-----------------------> The json result will be located in the same folder  of this script
    else:
        #script_path = path_json_result ###---------------------> The json result will be located in the path specified on the config file
        script_path = os.getcwd()
    #print ("The current working directory is %s" % script_path)
    if os.path.exists(script_path + "/result") is False:
        try:
            os.mkdir(script_path + "/result")
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']))
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename))
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename) + '/mysqlcapture')
        except OSError:
            print ("Creation of the directory %s failed" % path)
    elif os.path.exists(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version'])) is False:
        try:
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']))
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename))
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename) + '/mysqlcapture')
        except OSError:
            print ("Creation of the directory %s failed" % path)
    elif os.path.exists(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename)) is False:
        try:
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename))
            os.mkdir(script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename) + '/mysqlcapture')
        except OSError:
            print ("Creation of the directory %s failed" % path)

    path_json_result = script_path + "/result/" + str(vc_rpt_parse_stats__all['parse_version']) + '/' + str(dir_filename) + '/mysqlcapture/'

    with open(path_json_result + 'springer_qualifier.json', 'w', encoding ='utf8') as json_file:
        json.dump(data_to_json, json_file, default=str)

#-------------------------------------------------------------------------------------------------------------------------
def main():
    global cnx, cursor
    config, path_json_result = get_values_config()
    cnx = mysql.connector.connect(**config)
    #verify if the user added or not the ctl-dat argument
    if len(sys.argv) <= 1:
        print("Is necessary to specify a ctl-dat file name as an argument")
    else:
        #catch the argument
        vc_master__file_name = [sys.argv[1]]
        cursor = cnx.cursor(dictionary=True, buffered=True)
        #verify if the connection was done or not
        if cnx:
            print ("Connected Successfully")
            #Execute all queries by functions
            vc_master__func(vc_master__file_name, cursor)
            define_structure()
            cursor.close()
        else:
            print ("Connection Not Established")
            data_to_json = {}
            cursor.close()

        #write the ordered data in a json file
        create_json(define_structure(), path_json_result)

if __name__ == "__main__":
    main()