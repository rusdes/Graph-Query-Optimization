# class property graph

"""
This class implements property graph model and required functionalities to 
create property graph models.
It uses pygraphml library for underlying graph implementation.

Methods:

    1. add_vertex() : adds a vertex to the graph. If the vertex with same name already exists,
        it returns the existing vertex.

    2. add_edge() : adds edge between two vetices of graph

    3. add_vertices_and_edge() : adds two vertices and edge connecting them

 """

from pygraphml import Graph
from pygraphml import GraphMLParser
# from rdf_data_processor import rdf_data_processor

class Property_Graph:

    def __init__(self):
        
        """
        Initializes graph parameters.
        """
        self.g = Graph()
        self.vertex_id = 0
        self.edge_id = 0

    def add_vertex(self, label, value, property=None):

        """
        Adds a vertex to the graph.

        @param label: default label or key by which vertex is to be 
            added (vertex will be uniquely by this label and its value) e.g. "name"
        @param value: value of above label e.g. "product0012"
        @param property: list of length 2 containing key name of property as first element
            and value of property as second element
        """
        
        # check if node with same label already exists, if it does then return that node instance 
        for n in self.g._nodes:
            if n[label] == value:
                return n
        
        # add new node to graph
        node = self.g.add_node(str(self.vertex_id))
        # add default label and its value
        node[label] = value
        # add additional properties if provided
        if property != None:
            node[property[0]] = property[1]
        self.vertex_id += 1
        return node
    
    def add_edge(self, label, label_value1, label_value2, default_property, property=None):
        
        """
        Adds edge between two vertices.

        @param label: label or key value by which vertex will be searched e.g. "name" or "URI"
        @param label_value1 : value of above label or key of source vertex
        @param label_value2 : value of above label or key of target vertex
        @param default_property : default_property of edge (predicate in RDF terms)
        @param property : additional property, list of length 2 containing key name of property 
            as first element and value of property as second element
        """
        n1 = None
        n2 = None

        # Search source and target nodes
        for n in self.g._nodes:
            if n[label] == label_value1:
                n1 = n
            if n[label] == label_value2:
                n2 = n
        
        # If source or target doesn't exists, then return
        if n1 == None or n2 ==None:
            return

        # Add edge
        edge = self.g.add_edge(n1, n2, directed=True)
        # Add edge default property 
        edge['property'] = default_property
        edge['id'] = str(self.edge_id)
        # Add additional property if provided
        if property != None:
            edge[property[0]] = edge[property[1]]
        self.edge_id += 1


    def add_vertices_and_edge(self, label, label_value1, label_value2, default_edge_property,\
         edge_property=None, vertex1_property=None, vertex2_property=None):

        """
        Adds two vertices and edge connecting them

        @param label: default label or key by which vertex is to be 
            added (vertex will be uniquely by this label and its value) e.g. "name"
        @param label_value1 : value of above label or key of source vertex
        @param label_value2 : value of above label or key of target vertex
        @param default_edge_property : default_property of edge (predicate in RDF terms)
        @param edge_property : additional property, list of length 2 containing key name of property 
            as first element and value of property as second element
        @param vertex1_property: list of length 2 containing key name of property as first element
            and value of property as second element
        @param vertex2_property: list of length 2 containing key name of property as first element
            and value of property as second element
        """
        n1 = self.add_vertex(label, label_value1, vertex1_property)
        n2 = self.add_vertex(label, label_value2, vertex2_property)
        edge = self.g.add_edge(n1, n2, directed=True)
        edge['label'] = default_edge_property
        edge['id'] = str(self.edge_id)
        self.edge_id += 1
        if edge_property != None:
            edge[edge_property[0]] = edge[edge_property[1]]

    def add_property(self, label, label_value, property, to_edge=False):

        """
        Adds property to a edge or vertex 
        
        @param label : label or key by which desired edge or vertex will be searched
        @param label_value : value of above label or key
        @param property : property to be added, list of length 2 containing key name of 
            property as first element and value of property as second element
        @param to_edge : If set True, property will be added to edge, default value is False. 
        """
        if to_edge:
            # Yet to be implemented
            pass
        else:

            for n in self.g._nodes:
                if n[label] == label_value:
                    n[property[0]] = property[1]
                    break 

    def save_graph(self, file_name):

        """
        Save graph to .graphml file

        @param file_name : name of the file to which graph will be saved
        """
        parser = GraphMLParser()
        parser.write(self.g, file_name)


"""
Test code
"""
# if __name__ == "__main__":
    
#     con = Property_Graph()
#     con.add_vertex('URI', 'something_dot_com', ['name','myself'])
#     con.add_vertex('URI', 'something1_dot_com', ['name','urself'])
#     con.add_vertex('URI', 'something2_dot_com', ['name','herself'])
#     con.add_vertex('URI', 'something3_dot_com', ['name','himself'])
#     con.add_edge('URI','something_dot_com', 'something1_dot_com', 'knows')
#     con.add_edge('URI','something1_dot_com', 'something2_dot_com', 'knows')
#     con.save_graph("test_output.graphml")
    
    
    
    
    
    
    
    # class rdf_data_processor

"""
This class implements the funcions required for processing RDF data (currently only ntriple files).
It takes ntripple file as input.
Methods:
    1. read_line() : Returns next line from the file. Returns None at the end of file.

    2. process_line() : Returns subject, predicate, object and names of subject and object
        It also returns "found_non_URI_field" flag.  
"""


import numpy as np 
import re

class rdf_data_processor:

    def __init__(self, file, namespace=''):
        """
        Initializing.
        @param file : Ntriple (.nt) file 
        @param namespace : Namespace to extract names of entities. 
        """
        self.namespace = namespace
        self.file = file
        print("Opening data file")
        self.f = open(file)

    def read_line(self):
        """
        Returns next line from the file. Returns None at the end of file.
        """
        line = self.f.readline()
        if line == '':
            return None
        return line

    def process_line(self):
        """
        Returns subject, predicate, object and names of subject and object
        It also returns "found_non_URI_field" flag.
        """
        # read next line
        line = self.read_line()
        if line == None:
            return None
        # remove leading whitespaces if any
        line = line.lstrip()
        found_non_URI_field = False
        # split line into words
        words = line.split()
        for i in range(len(words)):
            # remove '<' and '>'
            words[i] = re.sub("<", "", words[i])
            words[i] = re.sub('>', "", words[i])
            length = len(words[i])
            # remove quotes ('"")
            words[i] = re.sub('"', "", words[i])
            # check if field is non-URI by checking the length before removing quotes is same or not
            # non-URI if length changes   
            if length != len(words[i]):
                found_non_URI_field = True
        
        names = []
        # first word in the line is subject
        subject = words[0]
        # second word in the line is predicate
        predicate = words[1]
        # third word in the line is object
        obj = words[2]
        # remove namespaces from subject and object to get names
        # if namespace is not provided, names will be same as subject and object 
        name = re.sub(self.namespace, "", subject)
        names.append(name)
        name = re.sub(self.namespace, "", obj)
        names.append(name)
        return [subject, predicate, obj], names, found_non_URI_field





# class database_mapper
"""
Class uses rdf_data_processor and property_graph classes to map 
RDF (ntriple) data to Property graph model data

Methods : 
    1. map_database() : Maps RDF data to Property graph model data.

    2. save_database() : Saves mapped database to .graphml file.
    
"""
# from rdf_data_processor import rdf_data_processor
# from property_graph import Property_Graph

class database_mapper(object):

    def __init__(self, input_file, namespace=''):
        """
        Initialization
        @param input_file : Input RDF (.nt) data file 
        @param namespace : Namespace to extract names of entities.
        """
        # Initialize graph 

        print("Initializing.... ")
        
        self.graph = Property_Graph()
    
        self.rdf_data_processor_ = rdf_data_processor(input_file, namespace=namespace)
        
        print("Initialized. ")
        # default label or key for vertex (here it is URI as sample RDF data provided has data 
        #  in the form of URI's)
        self.label = 'URI'
    
    def map_database(self):

        """
        Maps RDF data to Property graph model data. 
        """
        print("Mapping database.")
        while True:
            data = self.rdf_data_processor_.process_line()
            # check if data is None, (it is None at the end of file)
            if data == None:
                break
            rdf_data = data[0]
            print(rdf_data)
            # subject and object in RDF data will be vertices in property graph model 
            vertex1_property = [self.label, rdf_data[0]]
            vertex2_property = [self.label, rdf_data[2]]
            # predicate will be edge property connecting two verices
            edge_property = rdf_data[1]
            names = data[1]
            found_non_URI_field = data[2]
            # non-URI object in rdf data will be assigned as property of subject in graph data
            # for demonstration purpose, only non-URI objects are assigned as properties
            # this situation can be extended to any type of objects, e.g. object which is a country
            if found_non_URI_field == False:
                self.graph.add_vertices_and_edge('name', names[0], names[1], edge_property, \
                    vertex1_property=vertex1_property, vertex2_property=vertex2_property)
            else:
                self.graph.add_property('name', names[0], [rdf_data[1], names[1]])
        
    def save_database(self, output_file_name):
        
        print("Saving database....")
        self.graph.save_graph(output_file_name)
        print("Database saved to " + output_file_name + ".")
        


"""
Test code
"""
# if __name__ == "__main__":
#     data_file = '/home/rajviparekh/projects/BEproject/dbpedia dataset/unzipped/taxonomy.ttl'
#     # namespace = "http://db.uwaterloo.ca/~galuc/wsdbm/"
#     database_mapper_ = database_mapper(data_file)    #, namespace=namespace)
#     database_mapper_.map_database()
#     database_mapper_.save_database('graph1.graphml')






def convert_graphml_to_csv(source):
    import networkx as nx
    import matplotlib.pyplot as plt
    G = nx.read_graphml(source)
    vertex_list=[]
    for node in G.nodes(data=True):
        # print(node)
        node_id= node[0]
        label=''
        property=''
        for key,value in node[1].items():
            # print(key)
            property=property+key+','+value+','
        property= property.rstrip(property[-1])
        vertex_list.append([node_id, label, property])
    # print(vertex_list)
        
        
        
        
    edge_list=[]   
    for edge in G.edges(data=True):
        print(edge)
        
        edge_id= int(edge[2].get('id'))
        source_id=int(edge[0])
        target_id=int(edge[1])
        property=''
        label= edge[2].get('label')
        edge_list.append([edge_id, source_id,target_id,label, property])
        
    print(edge_list)
    
    
    import csv
  
  
    # field names 
    fields_vertex = ['ID','Labels','Properties'] 
    fields_edge= ['ID','sourceID','targetID','Label','Properties'] 
    
    with open('rdf_edges', 'w') as f:
        
        # using csv.writer method from CSV package
        write = csv.writer(f)
        
        write.writerow(fields_edge)
        write.writerows(edge_list)
        
        
    with open('rdf_vertices', 'w') as f:
        
        # using csv.writer method from CSV package
        write = csv.writer(f)
        
        write.writerow(fields_vertex)
        write.writerows(vertex_list)
    
    
    print("Converted to CSV")
    



#run example.py
    
import sys
# sys.path.append("../src")
# from database_mapper import database_mapper

if __name__ == "__main__":
    # sample data file
    input_file = '/home/rajviparekh/projects/BEproject/rdf_to_property_graph/data/sample_data.nt'
    graphml_output_file= 'graph.graphml'
    # csv_output_file= ''
   
    # namespace = "http://db.uwaterloo.ca/~galuc/wsdbm/"
    namespace=''
   
   
    # Initialization
   
    database_mapper_ = database_mapper(input_file, namespace=namespace)
    database_mapper_ = database_mapper(input_file)
   
    # Mapping database
    database_mapper_.map_database()
   
    # Saving database
    database_mapper_.save_database(graphml_output_file)
    convert_graphml_to_csv(graphml_output_file)
