import pandas as pd

from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


import json

from pprint import pprint

import sys, os

graph = Graph()
connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')


# The connection should be closed on shut down to close open connections with connection.close()
g = graph.traversal().withRemote(connection)
# Reuse 'g' across the application



# management list
# - metadata
# - schedule
# - phones
# - address

# - index search
# - metadata
# - name
# - alternate_name


def graphSetup():

    schema_msg = """mgmt = graph.openManagement()
                string_prop = mgmt.makePropertyKey('slist_1').dataType(String.class).cardinality(Cardinality.list).make()
                float_prop = mgmt.makePropertyKey('slist_2').dataType(String.class).cardinality(Cardinality.list).make()
                mgmt.commit()"""

    # connection.submit(schema_msg)

    schema_search = """mgmt = graph.openManagement()
                mgmt.buildIndex('metadataBySummary', Vertex.class).addKey('metadata', Mapping.TEXT.asParameter()).buildMixedIndex("search")
                mgmt.buildIndex('nameBySummary', Vertex.class).addKey('name', Mapping.TEXT.asParameter()).buildMixedIndex("search")
                mgmt.buildIndex('alternate_nameBySummary', Vertex.class).addKey('alternate_name', Mapping.TEXT.asParameter()).buildMixedIndex("search")
                mgmt.commit()"""

    connection.submit(schema_search)


def VertexLocation(filename):
    print("VertextLocation")

    df = pd.read_csv(filename, sep=',')
    print(list(df.columns))

    for i, row in df.iterrows():
        # print(df.loc[i, 'country'])
        zipdata = str(df.loc[i, 'zip'])
        latitude = df.loc[i, 'latitude']
        longitude = df.loc[i, 'longitude']
        city = df.loc[i, 'city']
        stateCode = df.loc[i, 'stateCode']
        state = df.loc[i, 'state']
        county = df.loc[i, 'county']
        country = df.loc[i, 'country']
        countryCodeIso2 = df.loc[i, 'countryCodeIso2']
        countryCodeIso3 = df.loc[i, 'countryCodeIso3']
        typedata = df.loc[i, 'type']
        metadata = ["city in " + state, "city in " + stateCode,
                    " city in " + countryCodeIso3,  "state in " + countryCodeIso3]

        g.addV(typedata).property('zip', zipdata).property('city', city).property('stateCode', stateCode).property('state', state).property('county', county).property('country', country).property('countryCodeIso2', countryCodeIso2).property('countryCodeIso3', countryCodeIso3).property('type', typedata).property('lat', latitude).property('lon', longitude).property('metadata', metadata).next()  # .property('cordinates',Geoshape.point(latitude, longitude)).next()

        print("Loaded >>> " + str(i))
    
    print(" <<<<>>>> Loaded all cities data")


def VertexCovidTestLocation(folderPath):
    for filename in os.listdir(folderPath):
        if filename.endswith(".json"): 
            file = os.path.join(folderPath, filename)
            with open(file) as f:
                data = json.load(f)

                for node in data:

                    name = node["name"]
                    alternate_name = node["alternate_name"]
                    description = node["description"]
                    featured = node["featured"]
                    phones = node['phones']
                    address = node['physical_address']
                    schedule = node['regular_schedule']
                    
                    typedata = 'covid-19 test center'
                    latitude = '0.0'
                    longitude = '0.0'

                    # add cordinates


                    postal_code = node['physical_address'][0]['postal_code']
                    state_province = node['physical_address'][0]['state_province']

                    metadata = ['Covid-19 Test centers in ' + state_province,'Covid-19 clinic', 'Covid-19 test location in US']

                    print("Loading  >>>>>>>>>> " +  postal_code)

                    try:
                        location = g.V().hasLabel('location').has('zip', postal_code).next()
                        testCenter = g.addV(typedata).property('name',name).property('alternate_name', alternate_name).property('description', description).property('featured',featured).property('phones', phones).property('address', address).property('schedule', schedule).property('metadata', metadata).property('type', typedata).property('zip', postal_code).property('lat', latitude).property('lon', longitude).next()
                        g.V(testCenter).addE('located_at').to(location).next()

                        print("Passed >>>>>>>>>>>> " + postal_code)
                    except:
                        print("Cannot pass this : " + postal_code)
                        # pass  
                            
                    print("======================================")
                
                print("$$$$$$$$$$$>>>>>>>>>> Done with Operations ", file)

#VertexLocation("Data/Location/alluscities.csv")
VertexCovidTestLocation("Data/Covid-19-test-location/")
connection.close()