# Parte Graphframes NoSQL
1. Ejecutar el comando adentro de la carpeta
`mvn clean package`
2. Situarse en el directorio `target` y enviarlo a Pampero:
`scp graphframes-tpe-1.jar kcortesrodrigue@pampero.it.itba.edu.ar:./`
3. En Pampero enviar el *jar al node1 
`scp graphframes-tpe-1.jar kcortesrodrigue@node1.it.itba.edu.ar:./` 
(accedan a mi cuenta. Password es igual que el username)
4. Situarse dentro de node1 (con ssh a mi usuario).
`spark-submit --master yarn  --deploy-mode=cluster --class ar.edu.itba.nosql.GraphFramesAppMain --jars hdfs:///user/kcortesrodrigue/graphframes-0.6.0.jar ./graphframes-tpe-1.jar <NUMERO_QUERY> <PATH_ABSOLUTO_CATEGORY> <PATH_ABSOLUTO_CATEGORIES> <PATH_ABSOLUTO_VENUES> <PATH_ABSOLUTO_STOPS>`. Para realizar esto mismo, deben estar todos los archivos en todos los nodos.
5. (Paso si está fuera del ITBA) En una terminal, hacer un proxy socks que escuche en localhost:9090
`ssh -D 9090 kcortesrodigue@pampero.it.itba.edu.ar`
6. (Paso si está fuera del ITBA) Entrar a `chrome://settings` y buscar la configuración del proxy. Settearlo con los valores puestos en el item anterior.
7. En `node1.it.itba.edu.ar:8080` ingresar con usuario/contraseña, y hacer
Spark2 >> Spark2 History Server UI >> clic en AppID recien ejecutado >> Executors >> Clic en primer stdout del lado de la derecha (donde dice driver)
