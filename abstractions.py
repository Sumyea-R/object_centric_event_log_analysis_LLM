import pm4py
import pandas as pd
import numpy as np
from pm4py.objects.log.obj import EventLog, EventStream
from typing import Union
from pm4py.utils import get_properties, constants
from pm4py.utils import __event_log_deprecation_warning
from pm4py.objects.ocel.obj import OCEL
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.ocel.util import related_objects
from pm4py import llm
import duckdb
import sqlite3


def ocel_object_interruption_abstraction(ocel: OCEL, activity: [str]) -> str:
    
    ocel = pm4py.filter_ocel_event_attribute(ocel, "ocel:activity", activity, positive=True)
    ocel = pm4py.ocel_o2o_enrichment(ocel)
    relations = ocel.relations.sort_values([ocel.event_timestamp, ocel.event_activity, ocel.object_id_column, ocel.event_id_column])
    o2o_relation = ocel.o2o.groupby('ocel:oid')['ocel:oid_2'].agg(set).to_dict()
    o2e_relation = relations.groupby(ocel.object_id_column)[ocel.event_id_column].agg(list).to_dict()
    e2o_relation = relations.groupby(ocel.event_id_column)[ocel.object_id_column].agg(list).to_dict()
    object_list = ocel.objects['ocel:oid']
    object_type_list = ocel.objects['ocel:type']
    object_to_type = {k: v for k, v in zip(object_list, object_type_list)}
    
    event_list = ocel.events['ocel:eid']
    event_activity = ocel.events['ocel:activity']
    event_timestamp = ocel.events['ocel:timestamp']
    event_to_activity_timestamp = {k : [v1, v2] for k, v1, v2 in zip(event_list, event_activity, event_timestamp)}
    
    params = ['', '', '', '', '', '', '', '']
    history = execution_history(ocel, params)
    return history


def resource_occupation_abstraction(ocel: OCEL, line: str, item: str) -> str:
    
    ocel = pm4py.ocel_o2o_enrichment(ocel)
    relations = ocel.relations.sort_values([ocel.event_timestamp, ocel.event_activity, ocel.object_id_column, ocel.event_id_column])  
    o2o_relation = ocel.o2o.groupby('ocel:oid')['ocel:oid_2'].agg(set).to_dict()
    o2e_relation = relations.groupby(ocel.object_id_column)[ocel.event_id_column].agg(list).to_dict()
    
    assembly_line_ocel = pm4py.filter_ocel_object_types(ocel, [line])
    assembly_line_objects = set(assembly_line_ocel.objects['ocel:oid'].to_list())
    assembly_line_dict = {k : v for k, v in o2o_relation.items() if k in assembly_line_objects}

    relations_item = relations[relations[ocel.object_type_column].isin([item])]
    event_list = relations_item[ocel.event_id_column]
    timestamp_list = relations_item[ocel.event_timestamp]
    object_list = relations_item[ocel.object_id_column]
    activity_list = relations_item[ocel.event_activity]
    event_dict = {k : [v1, v2, v3] for k, v1, v2, v3 in zip(event_list, object_list, timestamp_list, activity_list)}
    
    ret = ['']

    for lines in assembly_line_dict:
        stru = ''
        events = o2e_relation[lines]
        for event in events:
            stru = stru + 'Resource '+ lines + ' was occupied by item ' + event_dict[event][0] + ' at timestamp ' + str(event_dict[event][1]) + ' for ' + event_dict[event][2]
            stru = stru + '\n'
        ret.append(stru)
    
    ret = '\n'.join(ret)
    return ret


def object_creation_abstraction(ocel: OCEL) -> str: #creation
    
    obj_graph = pm4py.ocel.discover_objects_graph(ocel, graph_type='object_descendants')
    object_list = list(ocel.objects[ocel.object_id_column])
    object_type_list = list(ocel.objects[ocel.object_type_column])
    object_to_type = {k:v for k, v in zip(object_list, object_type_list)}
    ret = ['']
    
    ret.append("Beforehand, a bit of notions.")
    ret.append("Given an object-centric event log, the object interaction graph connects objects that are related in at least an event.")
    ret.append("The object descendants graph connects objects related in at least an event, when the lifecycle of the second object starts after the lifecycle of the first.")
    ret.append("The object inheritance graph connects objects when there an event that ends the lifecycle of the first object and starts the lifecycle of the second one.")
    ret.append('\n')
    ret.append("The following descriptions are based on object descendants graph.")
    ret.append("\n")
    
    for obj in obj_graph:
        stru = ''
        stru = stru + 'When ' + object_to_type[obj[0]] + ' ' + obj[0] + ' is alive, ' + object_to_type[obj[1]] + ' ' + obj[1] + ' is created.'
        stru = stru + '\n'
        ret.append(stru)
    
    ret = '\n'.join(ret)
    return ret


def object_continuation_abstraction(ocel: OCEL) -> str: #continue
    
    obj_graph = pm4py.ocel.discover_objects_graph(ocel, graph_type='object_inheritance')
    object_list = list(ocel.objects[ocel.object_id_column])
    object_type_list = list(ocel.objects[ocel.object_type_column])
    object_to_type = {k:v for k, v in zip(object_list, object_type_list)}
    ret = ['']
    
    ret.append("Beforehand, a bit of notions.")
    ret.append("Given an object-centric event log, the object interaction graph connects objects that are related in at least an event.")
    ret.append("The object descendants graph connects objects related in at least an event, when the lifecycle of the second object starts after the lifecycle of the first.")
    ret.append("The object inheritance graph connects objects when there an event that ends the lifecycle of the first object and starts the lifecycle of the second one.")
    ret.append('\n')
    ret.append("The following descriptions are based on object inheritance graph.")
    ret.append("\n")
    
    for obj in obj_graph:
        stru = ''
        stru = stru + object_to_type[obj[1]] + ' ' + obj[1] + ' is created starting from ' + object_to_type[obj[0]] + ' ' + obj[0] + ', ' + object_to_type[obj[0]] + ' ' + obj[0] + ' should not be modified.'
        stru = stru + '\n'
        ret.append(stru)
    
    ret = '\n'.join(ret)
    return ret


def execution_history(ocel: OCEL, params: []) -> str:
    
    object_list = list(ocel.objects[ocel.object_id_column])
    object_type_list = list(ocel.objects[ocel.object_type_column])
    object_to_type = {k:v for k, v in zip(object_list, object_type_list)}
    extended_table = ocel.get_extended_table()
    columns = [column for column in extended_table.columns if column.find(':') == -1]
    relations = ocel.relations.sort_values([ocel.event_timestamp, ocel.event_activity, ocel.object_id_column, ocel.event_id_column])
    event_to_object_relation = related_objects.related_objects_dct_overall(ocel)
    ret = ['']
    processed_objects = []
    ret.append('The following contains the history of process executions from an object centric event log, where each sentence represents an event and the ids within the sentences represents related objects.\n\n')
    
    for idx, row in extended_table.iterrows():
        stru = ''
        related_object = event_to_object_relation[row[ocel.event_id_column]]
        items = [item for item in related_object if object_to_type[item] == params[4]]
        related_objects_updated = [value for value in related_object if object_to_type[value] != params[4]]
        
        stru = stru + row[ocel.event_activity] + ', '
        for objects in related_objects_updated:
            object_type = object_to_type[objects]
            if object_type == params[0]:
                stru = stru + 'with purchase requisition id ' + objects + ', '
            elif object_type == params[3]:
                stru = stru + 'on line ' + objects + ', '
            elif object_type == params[2]:
                stru = stru + 'goods issue id ' + objects + ', '
            elif object_type == params[1]:
                stru = stru + 'with order id ' + objects + ', '
            elif object_type == params[5]:
                stru = stru + 'for delivery id ' + objects + ', '
            elif object_type == params[7]:
                stru = stru + 'payment id ' + objects + ', '
            elif object_type == params[6]:
                stru = stru + 'for invoice id ' + objects + ', '
            else:
                stru = stru + object_type + ' id ' + objects + ', '
        if items:
            stru = stru + 'involving item'
            if len(items) > 1:
                stru = stru + 's'
            for item in items:
                stru = stru + ' ' + item + ', '   
        if columns:
            for column in columns:
                stru = stru + column + ' ' + str(row[column]) + ', '
        stru = stru + 'on ' + str(row[ocel.event_timestamp]) + '.'
        stru = stru + '\n'
        ret.append(stru)
            
    ret = '\n'.join(ret)
    
    return ret


def temporal_ocel_abstraction(ocel: OCEL, grouper_freq: str) -> str:
    
    relations = ocel.relations.sort_values([ocel.event_timestamp, ocel.event_activity, ocel.object_id_column, ocel.event_id_column])
    temporal_ocel = pm4py.ml.extract_temporal_features_dataframe(relations, grouper_freq, ocel.event_activity, ocel.event_timestamp, ocel.event_id_column, ocel.event_timestamp, ocel.object_id_column)
    temporal_ocel = temporal_ocel.drop(columns=['num_events', 'average_finish_rate', 'average_waiting_time', 'average_sojourn_time', 'average_service_time'])
    temporal_ocel = temporal_ocel.rename(columns = {'unique_resources' : 'unique_objects', 'unique_cases': 'num_events'})
    
    grouper_str = ''
    if grouper_freq == 'Y':
        grouper_str = 'year'
    elif grouper_freq == 'M':
        grouper_str = 'month'
    elif grouper_freq == 'W':
        grouper_str = 'week'
    elif grouper_freq == 'D':
        grouper_str = 'day'
    
    ret = ['']
    
    for idx, row in temporal_ocel.iterrows():
        stru = 'There are '
        stru = stru + str(row['num_events']) + ' number of events with ' + str(row['unique_objects']) + ' unique objects and ' + str(row['unique_activities']) + ' unique activities for the ' + grouper_str + ' ending at ' + str(row['timestamp']) +'.'
        stru = stru + ' The average arrival rate of event is ' + str(row['average_arrival_rate']) 
        stru = stru + '\n'
        ret.append(stru)
    
    ret = '\n'.join(ret)
    
    return ret


def ocel2_hypotheses_generation(ocel_path: str, abstraction: str) -> str:
    
    ocel = pm4py.read_ocel2(ocel_path)
    ret = ['']
    
    stru = abstraction
    
    stru = stru + 'Below I present how an OCEL is stored in SQLIte database.\n'
    stru = stru + ocel2_tables_description(ocel_path)
    stru = stru + '\n\n'
    stru = stru + 'Can you make some, preferably 3 to 4 questions based events and objects information provided, that I can convert to SQL queries later?'
    
    ret.append(stru)
    ret = '\n'.join(ret)
    
    return ret


def ocel2_query_translation(ocel_path: str, query: str) -> str:
    
    ocel = pm4py.read_ocel2(ocel_path)
    ret = ['']
    
    stru = '\nBelow I present how an OCEL is stored in SQLIte database.'
    stru = stru + ocel2_tables_description(ocel_path)
    stru = stru + '\nIf you need to compute timestamp differences, please use the "julianday" function which gets the fractional number of days since noon in Greenwhich on November 24, 4714 B.C. The timestamps have the format "%Y-%m-%dT%H:%M:%S".'
    stru = stru + '\nIf you need to compute the duration of a lifecycle of an object, select the corresponding table according to the object type then compute the difference between the ocel_time of the last row where the object id appears and the first row where the object id appears.'
    stru = stru + '\nIIf you need to compute the variant of an object, take the event ids that have the object id in the corresponding object column in event_object table and then get the corresponding event types of the events ids from the event table. Show the event types and event ids in separate column.'
    stru = stru + '\nAgain said, the database is SQLite. In SQLite, there is no STDDEV function but you need to compute it as: SQRT(AVG(col*col) - AVG(col)*AVG(col)). Again use the julianday function for timestamp columns. The events are already sorted by timestamp, moreover you cannot use ORDER BY inside a concatenation operator.' 
    stru = stru + '\nFrom the mentioned information can you provide me a SQLite query that can tell ' + query
    
    ret.append(stru)
    ret = '\n'.join(ret)
    
    return ret

def ocel2_tables_description(ocel_path: str) -> str:
    
    ocel = sqlite3.connect(ocel_path)
    event_map_type = pd.read_sql("SELECT * from event_map_type", ocel)
    object_map_type = pd.read_sql("SELECT * from object_map_type", ocel)
    
    event_type = event_map_type['ocel_type'].to_list()
    event_type_map = event_map_type['ocel_type_map'].to_list()
    
    object_type = object_map_type['ocel_type'].to_list()
    object_type_map = object_map_type['ocel_type_map'].to_list()
    
    ret = ['']
    stru = '\n- we have a table called "event" containing the fields: "ocel_id" (the event id), "ocel_type" (the event type/activity).'
    stru = stru + '\n- we have a table called "object" containing the fields: "ocel_id" (the object id), "ocel_type" (the object type).'
    stru = stru + '\n- we have a table called "object_object" that stores relationship among different objects. The table contains the fields: "ocel_source_id" (the source object id), "ocel_target_id" (the target object id), and "ocel_qualifier" (a string explaining why such relationship exists).'
    stru = stru + '\n- we have a table called "event_object" that stores relationship between event ids and object ids. The table contains the fields: "ocel_event_id" (the event ids), "ocel_object_id" (the object ids), and "ocel_qualifier" (a string explaining why such relationship exists). If process execution (case) needs to be computed, then it is to be done from this table.'
    stru = stru + '\n- we have a table called "event_map_type" containing the fields "ocel_type" (the event type/activity) and "ocel_type_map" (a cleaned identifier of the event type)'
    stru = stru + '\n- we have '+ str(len(event_type)) + ' different event types tables. They are called, '
    for i in range(len(event_type_map)):
        stru = stru + '"event_' + event_type_map[i] + '", '
        
    stru = stru + '. Each event  type table contain 2 fields: "ocel_id" that stores the object id, "ocel_time" that stores the timestamps. '
    stru = stru + '\n- we have a table called "object_map_type" containing the fileds "ocel_type" (the object type) and "ocel_type_map" (a cleaned identifier of the object type)'
    stru = stru + '\n- we have '+ str(len(object_type)) + ' different object types tables. They are called, '
    for i in range(len(object_type_map)):
        stru = stru + '"object_' + object_type_map[i] + '", '
        
    stru = stru + '. Each object type table contain 2 fields: "ocel_id" that stores the object id, "ocel_time" that stores the timestamps. For computing lifecycle duration this tables should be used.'
    stru = stru + '\nFor formulating a query, please use the aforementioned tables and fields.'
    
    ret.append(stru)
    ret = '\n'.join(ret)
    
    return ret