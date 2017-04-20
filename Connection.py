from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "123"))
session = driver.session()

session.run("CREATE (a:City {name: {name}})",
              {"name": "Madrid"})