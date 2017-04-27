from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "123"))
session = driver.session()

"""creacion base de datos
session.run("CREATE (a:City {name: {name}})",
              {"name": "Madrid"})
session.run("CREATE (a:City {name: {name}})",
              {"name": "Barcelona"})
session.run("CREATE (a:City {name: {name}})",
              {"name": "A Coruna"})
session.run("CREATE (a:City {name: {name}})",
              {"name": "Cadiz"})
session.run("CREATE (a:City {name: {name}})",
              {"name": "Valencia"})

create relationship
Match (a:City{name:"Cadiz"}) with a as madriz
Match (b:City{name:"Barcelona"}) Create (madriz)-[Metodo:Aereo{distancia:700,tiempo:1.66}]->(b)

order by shortest time
Match p=(a:City{name:"Cadiz"})-[r*0..5]-(b:City{name:"A Coruna"})
With p, relationships(p) As rcoll
Return p, reduce(time=0,x IN rcoll| time + x.tiempo) AS totalTime
Order By totalTime
"""
for object in session.run("Match p=(a:City{name:'Cadiz'})-[r*0..5]-(b:City{name:'A Coruna'}) With p, relationships(p) As rcoll Return p, reduce(time=0,x IN rcoll| time + x.tiempo) AS totalTime Order By totalTime"):
    print " " .join([str(x) for x in (object.items()[0][1])])