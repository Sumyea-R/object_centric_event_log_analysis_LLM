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
import sys
import sqlite3
import abstractions


def paginate_output(output_string: str, lines_per_page=10):
    
    lines = output_string.split('\n')
    lines = list(filter(None, lines))
    total_lines = len(lines)
    current_line = 0

    while current_line < total_lines:
        for i in range(current_line, min(current_line + lines_per_page, total_lines)):
            print(lines[i])

        user_input = input(">>Press Enter to see more, or 'q' to quit: ")

        if user_input.lower() == 'q':
            break

        current_line += lines_per_page


def abstraction_selection(ocel: OCEL) -> str:
    
    ocel = pm4py.read_ocel2(ocel_path)
    abstraction_type = input(">>Input the desired abstraction from the following\n>>for ocel events, objects abstraction input 1\n>>for ocdfg abstraction input 2\n>>for object interruption abstraction input 3\n>>for resource occupation abstraction input 4\n>>for object creation abstraction input 5\n>>for object continuation abstraction input 6\n>>for process history abstraction input 7\n>>for temporal feature abstraction input 8\n")
    
    if abstraction_type == '1':
        abstraction = llm.abstract_ocel(ocel)
    if abstraction_type == '2':
        abstraction = llm.abstract_ocel_ocdfg(ocel)
    elif abstraction_type == '3' :
        acitivity = []
        n = int(input(">>Input number of activities related to interruption : "))
        for i in range(0, n):
            ele = input("Input the activity : ")
            # adding the element
            activity.append(ele) 
        abstraction = abstractions.ocel_object_interruption_abstraction(ocel, activity)
    elif abstraction_type == '4' :
        line = input(">>Input the object type associated with resource (if any) in the OCEL: ")
        item = input(">>Input the object type associated with items/elemnets (if any) in the OCEL: ")
        abstraction = abstractions.resource_occupation_abstraction(ocel, line, item)
    elif abstraction_type == '5' :
        abstraction = abstractions.object_creation_abstraction(ocel)
    elif abstraction_type == '6':
        abstraction = abstractions.object_continuation_abstraction(ocel)
    elif abstraction_type == '7':
        pr = input(">>Input the object type associated with purchase requisition (if any) in the OCEL: ")
        po = input(">>Input the object type associated with purchase orders in the (if any) OCEL: ")
        gi = input(">>Input the object type associated with goods issue (if any) in the OCEL: ")
        line = input(">>Input the object type associated with production line (if any) in the OCEL: ")
        item = input(">>Input the object type associated with items/elemnets (if any) in the OCEL: ")
        delivery = input(">>Input the object type associated with order delivery (if any) in the OCEL: ")
        invoice = input(">>Input the object type associated with invoice (if any) in the OCEL: ")
        payment = input(">>Input the object type associated with payment (if any) in the OCEL: ")
        params = [pr, po, gi, line, item, delivery, invoice, payment]
        abstraction = abstractions.execution_history(ocel, params)
    elif abstraction_type == '8':
        grouper_freq = input(">>Input the frequency: Y/M/W/D: ")
        abstraction = abstractions.temporal_ocel_abstraction(ocel, grouper_freq)
        
    return abstraction
    
def query_with_abstractions(ocel_path: str) -> str:
    
    ocel = pm4py.read_ocel2(ocel_path)
    abstraction = abstraction_selection(ocel)
    ret = ['']
    stru = ''

    query = input(">>Input your query:\n")
    
    stru = stru + 'The desired abstraction with the query is given below.\n'
    stru = stru + abstraction + '\n'
    stru = stru + query
    
    ret.append(stru)
    ret = '\n'.join(ret)
    
    execute_query = input(">>Do you want to execute the query over the abstraction on LLM: y/n?\n")
    if execute_query == 'y':
        resp = ''
        api_key = input(">>Input the gpt api key: ")
        openai_model = input(">>Input the openai model: ")
        resp = llm.openai_query(stru, api_key=api_key, openai_model=openai_model)
        return resp

    return ret


def execute_query(ocel_path: str, sql_query: str):
    
    conn = sqlite3.connect(ocel_path)
    df = pd.read_sql_query(sql_query, conn)
    
    return df


def translate_query(ocel_path: str):
    
    ocel = pm4py.read_ocel2(ocel_path)
    query = input(">>Input your query to translate:\n")
    stru = abstractions.ocel2_query_translation(ocel_path, query)
    api_key = input(">>Input the gpt api key: ")
    openai_model = input(">>Input the openai model: ")
    sql_query = llm.openai_query(stru, api_key=api_key, openai_model=openai_model)
    sql_query = ''
    print("The Translated SQL query:")
    print(stru)
    execute_query = input(">>Do you want to execute the sql query: y/n?\n")
    
    if execute_query == 'y':
        df = execute_query(ocel_path, sql_query)
        print(df)
        return



def generate_hypotheses(ocel_path: str):
    
    ocel = pm4py.read_ocel2(ocel_path)
    print("For query generation an abstraction needs to be selected\n")
    abstraction = abstraction_selection(ocel)
    stru = abstractions.ocel2_hypotheses_generation(ocel_path, abstraction)
    api_key = input(">>Input the gpt api key: ")
    openai_model = input(">>Input the openai model: ")
    resp = llm.openai_query(stru, api_key=api_key, openai_model=openai_model)
    resp = ''
    paginate_output(stru)
    
    while True:
        execute_query = input(">>Do you want to execute any generated sql query: y/n?\n")
        if execute_query == 'y':
            sql_query = input(">>Input the sql query:\n")
            df = execute_query(ocel_path, sql_query)
            print(df)
        else:
            return



def querying_ocel():
    
    ocel_path = input(">>Input the sqlite OCEL path: ")
    
    querying_method = input(">>Input the querying method from the following\n>>for selecting abstractions input 1\n>>for translating queries input 2\n>>for generation of queries input 3\n>>for executing an SQL query input 4\n")
    
    if querying_method == '1':
        paginate_output(query_with_abstractions(ocel_path))
        
    elif querying_method == '2':
        translate_query(ocel_path)
        
    elif querying_method == '3':
        generate_hypotheses(ocel_path)
        
    elif querying_method == '4':
        sql_query = input(">>Input the sql query: ")
        execute_query(ocel_path, sql_query)



querying_ocel()

