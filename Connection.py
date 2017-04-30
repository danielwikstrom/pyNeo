from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "123"))


def setTimeAndCost(name, values,session):
    session.run(
        "Match() - [r:Conexion]->()   where r.Tipo = " + name + " set r.Time = r.Distancia * " + str(
            values["Time"]) + " + " + str(values["Carga/Descarga"]) + " , r.Cost = " + str(
            values["Cost"]) + " * r.Distancia")


def createCiudades(session):
    session.run("CREATE (a:City {name: {name}})", {"name": "Madrid"})
    session.run("CREATE (a:City {name: {name}})",{"name": "Barcelona"})
    session.run("CREATE (a:City {name: {name}})", {"name": "A Coruna"})
    session.run("CREATE (a:City {name: {name}})",{"name": "Valencia"})
def createConexiones(session):
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:600,Tipo:'Avion'}]->(b)")
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:600,Tipo:'Carretera'}]->(b)")
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:600,Tipo:'Tren'}]->(b)")
    session.run("CREATE (a:City {name: {name}})",
                {"name": "Cadiz"})
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Cadiz'}) Create (madriz)-[Metodo:Conexion{Distancia:700,Tipo:'Avion'}]->(b)")
    session.run(
        "Match (a:City{name:'Cadiz'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:1200,Tipo:'Barco'}]->(b)")

    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Valencia'}) Create (madriz)-[Metodo:Conexion{Distancia:400,Tipo:'Carretera'}]->(b)")
    session.run(
        "Match (a:City{name:'Valencia'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:250,Tipo:'Carretera'}]->(b)")
    session.run(
        "Match (a:City{name:'Valencia'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:250,Tipo:'Tren'}]->(b)")

    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'A Coruna'}) Create (madriz)-[Metodo:Conexion{Distancia:700,Tipo:'Tren'}]->(b)")
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'A Coruna'}) Create (madriz)-[Metodo:Conexion{Distancia:700,Tipo:'Carretera'}]->(b)")

    session.run("CREATE (a:City {name: {name}})",
                {"name": "Salamanca"})
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Salamanca'}) Create (madriz)-[Metodo:Conexion{Distancia:400,Tipo:'Carretera'}]->(b)")

def findRute(origen,destino,maxTime,session):
    return session.run("Match p=(a:City{name:'"+origen+"'})-[r*0..5]-(b:City{name:'"+destino+"'}) With p, relationships(p) As rcoll with p, reduce(cost=0,x IN rcoll| cost + x.Cost) AS totalCost,reduce(time = 0,x in rcoll | time + x.Time)as totalTime where totalTime < "
                       +str(maxTime)+" return p,totalCost,totalTime Order By totalCost limit 1")


def getName(id, session):
    return session.run("match (n) where ID(n) =" + str(id) + " return n.name").data()[0]

if __name__ == "__main__":
    session = driver.session()
    session.run("match ()-[r]->() delete r")
    session.sync()
    session.run("match (a) delete a")
    session.sync()

    createCiudades(session)
    session.sync()
    createConexiones(session)
    session.sync()



    CarreteraValues = {"Time": 1 / 100.0, "Cost": 0.01, "Carga/Descarga": 5 / 60.0}
    TrenValues = {"Time": 5 / 600.0, "Cost": 0.008, "Carga/Descarga": 1 / 6.0}
    AvionValues = {"Time": 1 / 600.0, "Cost": 0.035, "Carga/Descarga": 2 / 3.0}
    BarcoValues = {"Time": 1 / 50.0, "Cost": 0.003, "Carga/Descarga": 1 / 3.0}

    setTimeAndCost("'Carretera'",CarreteraValues,session)
    setTimeAndCost("'Tren'",TrenValues,session)
    setTimeAndCost("'Avion'",AvionValues,session)
    setTimeAndCost("'Barco'",BarcoValues,session)
    session.sync()

    q = findRute("Cadiz","A Coruna",12,session)
    data = q.data()[0]

    rute = []
    print "Total time: "+str(data['totalTime']) + "h Coste: " + str(data['totalCost'])+"$"
    for node in data['p']:
        name = getName(node.start,session)['n.name']
        name2 = getName(node.end,session)['n.name']
        print name + "<->" + name2 + " Tipo: " + node.get("Tipo")
        rute.append({'inicio':name,'final':name2,'tiempo':node.get('Time'),'tipo':node.get("Tipo")})
    print rute
    session.run("CREATE (n:Vehiculo { tipo: 'Avion',libre:0})")
    session.run("CREATE (n:Vehiculo { tipo: 'Tren',libre:0})")
    session.sync()
    session.run("Match (a:City{name:'Madrid'}) with a as madriz Match (b:Vehiculo{tipo:'Tren'}) Create (madriz)-[Tipo:hasA]->(b)")
    session.run("Match (a:City{name:'Cadiz'}) with a as madriz Match (b:Vehiculo{tipo:'Avion'}) Create (madriz)-[Tipo:hasA]->(b)")

    session.sync()

    session.run("match (a:City{name:'Barcelona'})-[r:hasA]-(b:Vehiculo) where b.libre <= 0 and b.tipo = 'Tren' return  ID(b) as vehiculo,ID(r) as relacion limit 1")
    session.run("match (a:City{name:'Madrid'})-[r:hasA]-(b:Vehiculo) where b.libre <= 0 and b.tipo = 'Carretera' return  ID(b) as vehiculo,ID(r) as relacion limit 1")

    dunno = []
    values = rute[0]
        #mal esto de final porque no son direccionales las conexiones
    ast = session.run("match(a:City{name:{ciudad}})-[r:hasA]-(b:Vehiculo) where b.libre <= 0 and b.tipo = {tipo} " + "return ID(b) as vehiculo, ID(r) as relacion limit 1",{"ciudad":values['final'],"tipo":values['tipo']})
    session.sync()
    print type(ast)
    print ast.values()
'''ciudad

    #setTimeAndCost('"Carreteras"',CarreteraValues)



"""creacion base de datos

session.run("1)
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
#for object in session.run("Match p=(a:City{name:'Cadiz'})-[r*0..5]-(b:City{name:'A Coruna'}) With p, relationships(p) As rcoll Return p, reduce(time=0,x IN rcoll| time + x.tiempo) AS totalTime Order By totalTime"):
    #print " " .join([str(x) for x in (object.items()[0][1])])
'''