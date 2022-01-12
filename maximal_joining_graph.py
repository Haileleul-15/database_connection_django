# Written By
# Haileleul Haile
# Akashaya Krishnamoorthy

import psycopg2
from collections import defaultdict


# CHANGE THESE VALUES AND RUN PROGRAM
DATABASE_NAME = 'database'
USERNAME = 'username'
PASSWORD = 'password'


# run makes a database connection and collected tables with foreign key connection
# it creates a graph of the foreign table - primary table connections and returns
# a maximum joining component
# the graph is directed from foreign table to primary table

# the program outputs an SQL query, which returns the primary keys of tables connected
# in the maximum joining components by foreign key constrains
def run(databaseName, username, password):
   # establishing the connection
   conn = psycopg2.connect(
      database=DATABASE_NAME, user=USERNAME, password=PASSWORD, host='127.0.0.1', port='5432'
   )

# Collect foreign table - primary table connections on foreign key constraints
#############################################################################################
   # given tabel name as an input, the query returns it's primary key
   getPrimaryKeyOfTable = """ SELECT k.COLUMN_NAME
                              FROM information_schema.table_constraints t
                              LEFT JOIN information_schema.key_column_usage k
                              USING(constraint_name,table_schema,table_name)
                              WHERE t.constraint_type='PRIMARY KEY'
                                    AND
                                    t.table_name=%s;  """

   # SQL query that returns a table with foreign tables, foreign key constraint and primary tables
   getForeignKeysTable = """  select kcu.table_schema || '.' ||kcu.table_name as foreign_table,
                                    rel_tco.table_schema || '.' || rel_tco.table_name as primary_table,
                                    string_agg(kcu.column_name, ', ') as fk_columns
                              from information_schema.table_constraints tco
                              join information_schema.key_column_usage kcu
                                       on tco.constraint_schema = kcu.constraint_schema
                                       and tco.constraint_name = kcu.constraint_name
                              join information_schema.referential_constraints rco
                                       on tco.constraint_schema = rco.constraint_schema
                                       and tco.constraint_name = rco.constraint_name
                              join information_schema.table_constraints rel_tco
                                       on rco.unique_constraint_schema = rel_tco.constraint_schema
                                       and rco.unique_constraint_name = rel_tco.constraint_name
                              where tco.constraint_type = 'FOREIGN KEY'
                              group by kcu.table_schema,
                                       kcu.table_name,
                                       rel_tco.table_name,
                                       rel_tco.table_schema,
                                       kcu.constraint_name
                              order by kcu.table_schema,
                                       kcu.table_name;"""

   # Creating a cursor object using the cursor() method
   cursor = conn.cursor()

   cursor.execute(getForeignKeysTable)
   # Fetch all table names
   # remove 'public.' from the table names because they are returned as 'public.tablename'
   data = [(pair[0].replace('public.', ''),pair[1].replace('public.', ''), pair[2]) for pair in cursor.fetchall()]

   # store unique foreign_table names to a list
   foreign_tables = list({pairs[0] for pairs in data})

# create utility variables for future use
#############################################################################################
   # create a dictionary for public key foreign key combination
   # used to refer to the foreign key connecting two tables
   fkComb = {}
   for(ft,pt,fk) in data:
      fkComb[(ft,pt)] = fk

   # get primary keys of the primary tables
   primaryKeys = {}
   for ft,pt,fk in data:
      cursor.execute(getPrimaryKeyOfTable, (pt,))
      pk = cursor.fetchone()
      primaryKeys[pt] = pk[0]

# Build a graph, Build maximum joining graph
#############################################################################################
   # create a graph using a default dict and append the foreign tables as source node
   # and their corresponding primary tables as destination node
   G = defaultdict(list)
   for (ft,pt,fk) in data:
      G[ft].append(pt)
   # uncomment the following line to make the graph undirected
      # G[pt].append(ft)

   # print(G.items())

   # Closing the connection
   conn.close()

   def doShit(tableName, graph):
      component = []
      def rec(tableName):
         if tableName not in component:
            component.append(tableName)

            for table in graph[tableName]:
               rec(table)
      rec(tableName)
      return component

   components = [
      # doShit('public.' + tname[0], G)
      doShit(tname.replace('public.', ''), G)
      for tname in foreign_tables
   ]

   # this list contains all the tables in the maximum joining graph
   max_joining_components = max(components, key=len)
   # print(max_joining_graph)

   # find the graph connection of the maximum joining components
   max_joining_graph = defaultdict(list)
   for table in max_joining_components:
      if table in foreign_tables:
         for key, value in G.items():
            if key == table:
               max_joining_graph[table] = G[table]
   # print(tableRelations.items())

# Define a function given a maximum joining graph, it returns a query statement which joins
# tables in the maximum joining graph on the foreign key constrains and returns the primary
# key attributes
#############################################################################################
   def createQuery(max_joining_graph):
      foreign_tables = max_joining_graph.keys()
      primary_tables = list({x for v in max_joining_graph.values() for x in v})
      query = '''
            SELECT {}
            FROM {}
      '''
      select_clause = ''
      from_clause = [ft for ft in foreign_tables][0] + ' '
      join_clause = ''

      # prepare the select clause by adding the primary tables
      for ptable in primary_tables:
         select_clause += ptable + '.' + primaryKeys[ptable] + ' ' + 'AS ' + ptable + '_id '
         if ptable != primary_tables[-1]:
            select_clause += ', '

      # prepare join clause
      for ft in foreign_tables:
         # print(ft)
         for pt in max_joining_graph[ft]:
            if pt in ft:
               join_clause += 'LEFT JOIN ' + pt + ' AS ' + pt[0] + ' ON ' +\
                  pt + '.' + primaryKeys[pt] + ' = ' + ft[0] + '.' + fkComb[(ft,pt)] + '\n'
            else:
               join_clause += 'LEFT JOIN ' + pt + ' ON ' +\
                  pt + '.' + primaryKeys[pt] + ' = ' + ft + '.' + fkComb[(ft,pt)] + '\n'
      
      # add the select and from clauses
      query = query.format(select_clause, from_clause)
      # add join clauses
      query = query + join_clause + ';'
   
      print(query)
      return query

   createQuery(max_joining_graph)

run(DATABASE_NAME, USERNAME, PASSWORD)