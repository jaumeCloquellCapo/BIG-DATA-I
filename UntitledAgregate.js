db.getCollection("restaurants").aggregate([
    { "$group": {
        "_id": "$address.zipcode",
		"ciudad1":{$push: {citID: "$restaurant_id", nomcity:"$name", lat: { $arrayElemAt: ["$address.coord", 1]},lon:{ $arrayElemAt: ["$address.coord", 0]}}},
		"ciudad2":{$push: {citID: "$restaurant_id", nomcity:"$name", lat: { $arrayElemAt: ["$address.coord", 1]},lon:{ $arrayElemAt: ["$address.coord", 0]}}}
		}
    },
 	{$unwind: {path: "$ciudad1" }},
	{$unwind: {path: "$ciudad2" }},
	{"$project": {_id: 0, Pais: "$_id", ciudad1: "$ciudad1.nomcity", ciudad2: "$ciudad2.nomcity", // esta linea falla y en la siguiente no añade ninguna parjea porque todos tienen valor 0.0
	    distancia:{ $sqrt: {$sum: [{$pow: [{$subtract: ["$ciudad1.lat","$ciudad2.lat"]},2 ]},
					{$pow: [{ $subtract: ["$ciudad1.lon","$ciudad2.lon" ]},2 ]}]}}
	    }
	},
	{$redact: {"$cond": [ {$and:[{"$lt": ["$ciudad1", "$ciudad2"]},{"$ne":["$distancia",0.0]}]},"$$KEEP","$$PRUNE"]}},
	{$group: {_id: "$Pais", "contador":{$sum:1}, "dist_min": {$min: "$distancia"}, "parejas":{$push: {ciudad1: "$ciudad1", ciudad2: "$ciudad2", distancia: "$distancia"}}}},
	{$unwind: "$parejas"}, // Desanidamos el “array” parejas
	{$redact: {"$cond": [{"$eq": ["$dist_min", "$parejas.distancia"]}, "$$KEEP", "$$PRUNE"]}},
	{$project: {_id: 0, "count" :"$contador", "ID": "$_id", "Rest1": "$parejas.ciudad1", "Rest2": "$parejas.ciudad2", "distancia": "$dist_min"}},
	{ $out : "rest_aggregate" }
], {
allowDiskUse: true, // Permite el uso de disco para operaciones intermedias que no quepan en memoria
cursor: { batchSize: 180 }
})
