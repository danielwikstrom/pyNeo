from neo4j.v1 import GraphDatabase, basic_auth
import sys
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "123"))
URGENT_DAY=4
URGENT_NIGHT=6
URGENT_MORNING=8
STANDARD=12
ECONOMIC=sys.maxint

def setTimeAndCost(name, values,session):
    session.run(
        "Match() - [r:Conexion]->()   where r.Tipo = " + name + " set r.Time = r.Distancia * " + str(
            values["Time"]) + " + " + str(values["Carga/Descarga"]) + " , r.Cost = " + str(
            values["Cost"]) + " * r.Distancia")

def createCiudades(session):
    session.run("CREATE (a:City {name: {name},bestTime:{bestTime}})", {"name": "Madrid","bestTime":4})
    session.run("CREATE (a:City {name: {name},bestTime:{bestTime}})",{"name": "Barcelona","bestTime":4})
    session.run("CREATE (a:City {name: {name},bestTime:{bestTime}})", {"name": "A Coruna","bestTime":8})
    session.run("CREATE (a:City {name: {name},bestTime:{bestTime}})",{"name": "Valencia","bestTime":8})
    session.run("CREATE (a:City {name: {name},bestTime:{bestTime}})", {"name": "Salamanca", "bestTime": 4})
    session.run("CREATE (a:City {name: {name},bestTime:{bestTime}})", {"name": "Cadiz", "bestTime": 12})
def createConexiones(session):
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:600,Tipo:'Avion'}]->(b)")
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:600,Tipo:'Carretera'}]->(b)")
    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Barcelona'}) Create (madriz)-[Metodo:Conexion{Distancia:600,Tipo:'Tren'}]->(b)")

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


    session.run(
        "Match (a:City{name:'Madrid'}) with a as madriz Match (b:City{name:'Salamanca'}) Create (madriz)-[Metodo:Conexion{Distancia:400,Tipo:'Carretera'}]->(b)")

def findRute(origen,destino,maxTime,session):
    return session.run("Match p=(a:City{name:'"+origen+"'})-[r*0..5]-(b:City{name:'"+destino+"'}) With p, relationships(p) As rcoll with p, reduce(cost=0,x IN rcoll| cost + x.Cost) AS totalCost,reduce(time = 0,x in rcoll | time + x.Time)as totalTime where totalTime < "
                       +str(maxTime)+" return p,totalCost,totalTime Order By totalCost limit 1")

def findRuteTEst(origen,destino,maxTime,session):
    return session.run("Match p=(a:City{name:'"+origen+"'})-[r*0..5]-(b:City{name:'"+destino+"'}) With p, relationships(p) As rcoll with p, reduce(cost=0,x IN rcoll| cost + x.Cost) AS totalCost,reduce(time = 0,x in rcoll | time + x.Time)as totalTime where totalTime < "
                       +str(maxTime)+" return nodes(p) as p,totalCost,totalTime Order By totalCost limit 1")


def getName(id, session):
    return session.run("match (n) where ID(n) =" + str(id) + " return n.name").data()[0]


def createClient(name, session):
        q=session.run("CREATE (a:Client {name:'"+name+"'}) return ID(a) as identificador").data()[0]
        return q['identificador']

def getNamebyId(id,session):
        q=session.run("Match (a:Client) where ID(a)="+str(id)+" return a.name as nombre").data()[0]
        return q['nombre']
def getTypeTransport(type):
    if(type is URGENT_DAY):
        ttype="urgentDay"
    elif(type is URGENT_NIGHT):
        ttype = "urgentNight"
    elif (type is URGENT_MORNING):
        ttype = "urgentMorning"
    elif (type is STANDARD):
        ttype = "standard"
    elif (type is ECONOMIC):
        ttype = "economic"
    return ttype
def order(id,origin, destination, type, session):
    t = session.run("match (c:Client) where ID(c)="+str(id)+" return c").data()
    if (not t):
        print("no username has that id")
        return None
    q = session.run("Match (n:City{name:'" + origin + "'}) return n.bestTime as bestTime").data()[0]

    tiempoMin = q['bestTime']

    p=findRute(origin,destination,type,session).data()[0]
    mejorTiempo=p['totalTime']

    if (tiempoMin <= type and type>=mejorTiempo):

        s=session.run("CREATE (n:Package{clientID:" + str(id) + ",cost:"+str(p['totalCost'])+",time:"+str(mejorTiempo)+",transports:[],location:'"+origin+"',type:'"+getTypeTransport(type)+"',payed:'"+str(False)+"'}) return ID(n) as identificador").data()[0]
        sID = s['identificador']
        for node in p['p']:
            trans=node.properties.values()[2]
            session.run("Match (n:Package) where ID(n)="+str(sID)+" set n.transports=n.transports+'"+trans+"'")
        session.run("Match (n:Package) where ID(n)="+str(sID)+" with n as cliente Match (b:Client) where ID(b)=" + str(id) + " Create (cliente)-[c:Order]->(b)")

        print("A new Package has been ordered: ")
        print("Price: "+ str(p['totalCost']))
        print("Time: "+ str(mejorTiempo)+ " hours")
        for node in p['p']:
            print str(getName(node.start, session)) + "<->" + str(getName(node.end, session))
        return sID
    else:
        print("The office in that area doesnt ship to your destination in less than "+ str(mejorTiempo)+ " hours")
        return None
def getPackageLocation(pId,session):
    p=session.run("match (n:Package) where ID(n)="+str(pId)+" return n.location as location").data()
    if(not p):
        print("that package doesnt exist")
    else:
        return p[0]['location']
def payService(idService,session):
    session.run("Match (n:Package) where ID(n)="+str(idService)+" set n.payed="+str(True))
def getServicesByType(idClient,session,*types):
    services=[]
    for x in types:
        s=session.run("match (n:Package)-[]-(a:Client) where ID(a)="+str(idClient)+" with n as services match (services) where services.type='"+x+"' return ID(services) as id").data()
        if(s):
            services.append(s[0]['id'])
    return services
def getPayedServices(idClient,payed,session):
    services=[]
    if(payed):
        s = session.run("match (n:Package)-[]-(a:Client) where ID(a)=" + str(idClient) + " with n as services match (services) where services.payed='true' return collect(ID(services)) as id").data()
        if(s):
            services=s['id']
            print("the following services have been payed: ")
            for x in services:
                print("service: "+str(x))
    else:
        prices=[]
        s = session.run("match (n:Package)-[]-(a:Client) where ID(a)=" + str(
            idClient) + " with n as services match (services) where services.payed='False' return collect(ID(services))as id,collect(services.cost) as price").data()
        if(s):
            services=s[0]['id']
            prices=s[0]['price']
            print("the following services have not been payed")
            for i,item in enumerate(services):
                print("service: "+ str(services[i])+", to pay: "+str(prices[i]))



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


    #session.run("CREATE (n:Service{clientID:"+str(94)+",cost:7,time:5}) with n as cliente Match (b:Client) where ID(b)="+ str(94)+ " Create (cliente)-[c:Order]->(b)")
    idC=createClient("Pepito",session)
    idS=order(idC,"Cadiz","Madrid",STANDARD,session)

    idS2 = order(idC, "Madrid", "Barcelona", URGENT_DAY, session)
    idS3 = order(idC, "Cadiz", "A Coruna", ECONOMIC, session)
    """
    """
    tipes=[]
    tipes.append("economic")
    tipes.append("urgentDay")
    tipes.append("blabla")
    getPayedServices(142,False,session)


    #getServicesByType(142,session,*tipes)
    #for x in tipes:
    #    session.run()

    q = findRute("Cadiz","A Coruna",99,session)
#print (type(q.data()))
    
    data = q.data()[0]
    print data['p']
    print data.values()



    q = findRute("Cadiz","A Coruna",12,session)
    data = q.data()[0]


    rute = []
    print "Total time: "+str(data['totalTime']) + "h Coste: " + str(data['totalCost'])+"$"

    for node in data['p']:
        name = getName(node.start,session)['n.name']
        name2 = getName(node.end,session)['n.name']
        print name + "<->" + name2

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
    print ast.data()
'''
ciudad

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