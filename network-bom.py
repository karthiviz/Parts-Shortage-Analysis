# -*- coding: utf-8 -*-
"""
Created on Wed Apr 28 07:34:56 2021

@author: 1026313
"""
import pandas as pd
import networkx as nx
import sqlite3
import json
from pandas import json_normalize

def create_network():
    sql = "SELECT bom.bom_id, bom.parent_item_name, bom.child_item_name, bom.quantity_per, bom.lead_time FROM showcase_bom as bom;"
    with sqlite3.connect("C:/Users/1026313/Documents/Analytics 2.0/BOM/showcase/showcase.db") as connection:
        edge_list = pd.read_sql(sql, connection)
    G = nx.from_pandas_edgelist(edge_list, source='child_item_name', target='parent_item_name', edge_attr=['bom_id', 'lead_time','quantity_per'], create_using=nx.DiGraph())
    connection.close()
    return G

def add_node_attr(G, affected_node, on_hand=0, demand=0, planned_inbound=0, planned_production=0, projected_inventory=0):
    sql = f"SELECT item_name, sell_price, buy_price, critical_item, alternate_item FROM showcase_dim_items as items where item_name = '{affected_node}';"
    with sqlite3.connect("C:/Users/1026313/Documents/Analytics 2.0/BOM/showcase/showcase.db") as connection:
        node_attr = pd.read_sql(sql, connection)
    node_attr_dict = node_attr.set_index('item_name').to_dict('index')
    node_attr_dict[affected_node]['on_hand'] = on_hand
    node_attr_dict[affected_node]['demand'] = demand
    node_attr_dict[affected_node]['planned_inbound'] = planned_inbound
    node_attr_dict[affected_node]['planned_production'] = planned_production
    node_attr_dict[affected_node]['projected_inventory'] = projected_inventory
    nx.set_node_attributes(G, node_attr_dict)
    connection.close()
    return G

def getParent(affected_node, date, on_hand, demand, planned_inbound, planned_production, projected_inventory):
    G = create_network()
    DG = add_node_attr(G, affected_node, on_hand, demand, planned_inbound, planned_production, projected_inventory)
    parents = list(DG.neighbors(affected_node))
    child_onhand = nx.get_node_attributes(DG, 'on_hand')[affected_node]
    child_inbound = nx.get_node_attributes(DG, 'planned_inbound')[affected_node] or 0
    return DG, parents, child_onhand, child_inbound


def getAttributes(DG, affected_node, parent, date):
    sql = f"SELECT m.node_inventory_derived_id, m.measure_date, m.on_hand, m.demand, m.planned_inbound, m.planned_production, m.projected_inventory, master.customer_item_name, master.criticalities FROM node_inventory_measures as m LEFT JOIN node_inventory_master as master ON master.derived_id = m.node_inventory_derived_id WHERE master.customer_item_name = '{parent}' AND m.measure_date = '{date}';"
    with sqlite3.connect("C:/Users/1026313/Documents/Analytics 2.0/BOM/showcase/showcase.db") as s_connection_1:
        node_measures = pd.read_sql(sql, s_connection_1, parse_dates='measure_date')
    p_on_hand = float(node_measures['on_hand'])
    p_demand = float(node_measures['demand'])
    p_planned_inbound = node_measures['planned_inbound']
    p_planned_production = node_measures['planned_production']
    p_projected_inventory = node_measures['projected_inventory']
    DG = add_node_attr(DG, parent, p_on_hand, p_demand, p_planned_inbound, p_planned_production, p_projected_inventory)
    parent_demand = nx.get_node_attributes(DG, 'demand')[parent]
    parent_onhand = nx.get_node_attributes(DG, 'on_hand')[parent]
    parent_req =  parent_demand - parent_onhand
    if nx.get_edge_attributes(DG, 'quantity_per')[(affected_node,parent)] * parent_req < 0:
        child_req = 0
    else: child_req = float(nx.get_edge_attributes(DG, 'quantity_per')[(affected_node,parent)] * parent_req)
    s_connection_1.close()
    return p_on_hand, p_demand, p_planned_inbound, p_planned_production, p_projected_inventory, parent_demand, parent_onhand, parent_req, child_req

def getImpact(affected_node, date, on_hand, demand, planned_inbound, planned_production, projected_inventory):
    DG, parents, child_onhand, child_inbound = getParent(affected_node, date, on_hand, demand, planned_inbound, planned_production, projected_inventory)
    print(f"{affected_node} on hand: {child_onhand}")
    print(f"{affected_node} inbound: {child_inbound}")
    data = {"impact": {'affected_parent':affected_node,'isCritical':False, 'monetary_impact':0}}
    for parent in parents:
        p_on_hand, p_demand, p_planned_inbound, p_planned_production, p_projected_inventory, parent_demand, parent_onhand, parent_req, child_req = getAttributes(DG, affected_node, parent, date)
        print(f"{parent} demand: {parent_demand}")
        print(f"{parent} on hand: {parent_onhand}")
        if parent_req > 0:
            print(f"{parent} extra req: {parent_req}")
        else: print(f"{parent} has excess: {-parent_req}")
        print(f"{affected_node} demand: {child_req}")
        if child_req >= child_onhand + float(child_inbound):
            print(f"{parent}'s demand for {affected_node} is NOT fully satisfied")
            possible_parent_qty = child_onhand / nx.get_edge_attributes(DG, 'quantity_per')[(affected_node,parent)]
            possible_parent_qty = possible_parent_qty / len(parents)
            sell_price = nx.get_node_attributes(DG, 'sell_price')[parent]
            monetary_impact = (parent_req - possible_parent_qty) * float(sell_price)
            data = {"impact": {'affected_parent':parent,'isCritical':child_req >= child_onhand + float(child_inbound), \
                              'monetary_impact':monetary_impact}}               
        else:
            print(f"{parent}'s demand for {affected_node} is satisfied")
    json_object = json.dumps(data, indent = 2)
    return json_object

def getImpactedObjects(affected_node, date, on_hand, demand, planned_inbound, planned_production, projected_inventory):
    DG, parents, child_onhand, child_inbound = getParent(affected_node, date, on_hand, demand, planned_inbound, planned_production, projected_inventory)
    filtered_mon_dict = {"salesOrder":{"object_id":None, "customer":None}}
    with sqlite3.connect("C:/Users/1026313/Documents/Analytics 2.0/BOM/showcase/showcase.db") as s_connection_2:
        for parent in parents:
            p_on_hand, p_demand, p_planned_inbound, p_planned_production, p_projected_inventory, parent_demand, parent_onhand, parent_req, child_req = getAttributes(DG, affected_node, parent, date)
            if child_req >= child_onhand + float(child_inbound):
                getOrderSql = "SELECT measure_date, object_id, customer_item_name AS parent, CAST(measure AS TEXT) as measure  FROM node_inventory_monetary_impact"
                monetary_impact_node_df = pd.read_sql(getOrderSql, s_connection_2)
                filtered_mon_impact = monetary_impact_node_df.loc[(monetary_impact_node_df.measure_date == date) & (monetary_impact_node_df.parent == parent)]
                filtered_mon_impact = filtered_mon_impact.set_index('measure').drop(['measure_date','parent'], axis=1)
                filtered_mon_json = filtered_mon_impact.to_json(orient='index', indent=2).replace("\n", "")
                filtered_mon_dict = json.loads(filtered_mon_json)
                object_id = filtered_mon_dict['salesOrder']['object_id']
                getCustomerSql = f"SELECT Customer FROM salesOrders WHERE OrderNo = '{object_id}';"
                customer_name = pd.read_sql(getCustomerSql, s_connection_2)['Customer'][0]
                filtered_mon_dict['salesOrder']['customer'] = customer_name
            else:
                continue
    s_connection_2.close()
    json_record = {"measures": {"salesOrder":filtered_mon_dict['salesOrder']}}
    json_object = json.dumps(json_record, indent = 2)
    return json_object
        

sql = "SELECT m.node_inventory_derived_id, m.measure_date, m.on_hand, m.demand, m.planned_inbound, m.planned_production, m.projected_inventory, master.customer_item_name, master.criticalities FROM node_inventory_measures as m LEFT JOIN node_inventory_master as master ON master.derived_id = m.node_inventory_derived_id;"
with sqlite3.connect("C:/Users/1026313/Documents/Analytics 2.0/BOM/showcase/showcase.db") as main_conn:
    node_measures_df = pd.read_sql(sql, main_conn)
    node_measures_df['impact'] = node_measures_df.apply(lambda row:\
                                    getImpact(row['customer_item_name'],\
                                    row['measure_date'], row['on_hand'],\
                                    row['demand'], row['planned_inbound'],\
                                    row['planned_production'], \
                                    row['projected_inventory']), axis=1)
    
    # node_measures_df['measures'] = node_measures_df.apply(lambda row:\
    #                                 getImpactedObjects(row['customer_item_name'],\
    #                                 row['measure_date'], row['on_hand'],\
    #                                 row['demand'], row['planned_inbound'],\
    #                                 row['planned_production'], \
    #                                 row['projected_inventory']), axis=1)
    
    # node_measures_df['measure_date'] = pd.to_datetime(node_measures_df['measure_date'])
        
main_conn.close()

cols = ['affected_parent', 'isCritical', 'monetary_impact']
days_between_list = []
for item in node_measures_df['customer_item_name']:
        filtered_df = node_measures_df.loc[node_measures_df['customer_item_name'] == item][['customer_item_name','measure_date','impact']]

        parsed_df = pd.DataFrame([json_normalize(json.loads(js)).values[0] for js in filtered_df['impact']],
                                 columns=cols)
    
        parsed_df['measure_date'] = filtered_df['measure_date'].values
        parsed_df['customer_item_name'] = filtered_df['customer_item_name'].values
        parsed_df = parsed_df.set_index('customer_item_name').\
                    drop('monetary_impact', axis=1)
        parsed_df = parsed_df.loc[parsed_df['isCritical'] == True]
        if not parsed_df.empty:
            parsed_df['max_days_between'] = max(parsed_df.measure_date)-min(parsed_df.measure_date)
            parsed_df['max_days_between'] = parsed_df['max_days_between'].dt.days + 1
            parsed_df.drop('isCritical', axis=1, inplace=True)
            days_between_list.append(parsed_df)
    
days_between_df = pd.concat(days_between_list)
merge_on = ['customer_item_name','measure_date']

merged_df = node_measures_df.merge(days_between_df, how='left', left_on=merge_on, right_on=merge_on)
merged_df.drop('affected_parent', axis=1, inplace=True)    

merged_df.to_excel('comp_shortage_showcase.xlsx', sheet_name='m_impact',\
                              index=False)






            
        
        
