
///ejercicio propuesto:
db.runCommand({
    aggregate: "restaurants",
    pipeline : [
       //{$match: {"$elemMatch: {"$gte": grades.score" : 5}}, //Solo considero los registros que hayan tenido al menos una calificacion "A"
       {$match: {"grades.score": {"$gte": 5}}}, 
       //Agrupa por código de cocina y le añade los arrays rest1 y rest2 con los datos de los restaurantes de ese tipo de cocina
       {$group: {_id: "$address.zipcode",
           "rest1":{$push: {resID: "$restaurant_id", nombre:"$name", dir:"$address"}}, //, lat:"$Latitude",lon:"$Longitude"}},
           "rest2":{$push: {resID: "$restaurant_id", nombre:"$name", dir:"$address"}}}},
       {$unwind: "$rest1"}, //Desanida rest1, crea un documento por cada elemento del array rest1
       {$unwind: "$rest2"}, //Desanida rest2 crea un documento por cada elemento del array rest2
        //Calcula la distancia entre cada par de restaurantes en el campo “distancia”, devuelve otros datos necesarios.
       {$project: {_id: 0, zipcode: "$_id", rest1: "$rest1", rest2: "$rest2",
             distancia:{ $sqrt: {$sum: [{$pow: [{$subtract: [{$arrayElemAt: ["$rest1.dir.coord",0]},{$arrayElemAt: ["$rest2.dir.coord",0]}]},2 ]},
                                        {$pow: [{$subtract: [{$arrayElemAt: ["$rest1.dir.coord",-1]},{$arrayElemAt: ["$rest2.dir.coord",-1]}]},2 ]}]}}}},
          // Eliminamos parejas de ciudades redundantes y aquellas parejas que están a distancia 0.
       {$redact: {"$cond": [{$and:[{"$lt": ["$rest1.resID", "$rest2.resID"]}]},"$$KEEP","$$PRUNE"]}},
       {$group: {_id: "$zipcode", "dist_min": {$min: "$distancia"}, // Obtenemos las distancia mínima para cada país
                                
                                // Añadimos a la salida un “array” con los datos de todas las parejas de ciudades de cada País
                               "parejas":{$push: {rest1: "$rest1", rest2: "$rest2", distancia: "$distancia"}}}},
       {$unwind: "$parejas"}, // Desanidamos el “array” parejas
        // Nos quedamos con aquellas parejas cuya distancia coincide con la distancia mínima de ese país
       {$redact: {"$cond": [{"$eq": ["$dist_min", "$parejas.distancia"]}, "$$KEEP", "$$PRUNE"]}},
       {$group: {_id: "$_id", "dist_min": {$min: "$dist_min"}, // Obtenemos las distancia mínima para cada país
           "contador": { $sum: 1 },
                                // Añadimos a la salida un “array” con los datos de todas las parejas de ciudades de cada País
                               "parejas":{$push: "$parejas"}}},
       // Proyectamos sobre los datos solicitados
       {$project: {_id: 0, "zipcode": "$_id", "contador": "$contador", "parejas": "$parejas", "distancia": "$dist_min"}},
       { $out : "rest_aggregate" }
      ],
     cursor: { batchSize: 20000} ,
     allowDiskUse: true} // Permite el uso de disco para operaciones intermedias que no quepan en memoria
    );