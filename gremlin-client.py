from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

graph = Graph()
connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')


# The connection should be closed on shut down to close open connections with connection.close()
g = graph.traversal().withRemote(connection)
# Reuse 'g' across the application

user_1 = g.addV('people').property('name','person6').next()

print(user_1.id)
print(user_1.label)

connection.close()
