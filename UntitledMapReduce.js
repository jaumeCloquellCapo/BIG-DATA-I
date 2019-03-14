db.runCommand({ mapReduce: "restaurants",
 map : function Map() {
 var key = this.address.zipcode;
 emit(key, {
  "data": [
   {
    "name" : this.name,
    "lat"  : this.address.coord[0],
    "lon"  : this.address.coord[1],
    "direccion" : this.address.street
   }
  ],
  "count": 1
 });
},
 reduce : function Reduce(key, values) {
    var data = [];
    var count = 0;
    values.forEach(function(value)  {
      count += value.count;
      value.data.forEach(function(restaurant) {
            data.push(restaurant);
      });
    });
    return { data: data, count: count}
},
 finalize : function Finalize(key, reduced) {
 if (reduced.data.length == 1) {
  return { "message" : "[Error ] Only one restaurant" };
 }
 var min_dist = Number.POSITIVE_INFINITY;
 var soluciones = []
 var c1;
 var c2;
 var d;
 for (var i in reduced.data) {
  for (var j in reduced.data) {
   if (i>j) continue;
   c1 = reduced.data[i];
   c2 = reduced.data[j];
   d = (c1.lat-c2.lat)*(c1.lat-c2.lat)+(c1.lon-c2.lon)*(c1.lon-c2.lon);
   if (d == min_dist && c1.name!=c2.name){
        var comp = {"restaurante 1": c1.name, "restaurante 2":c2.name, "direccion 1": c1.direccion, "direccion 2": c2.direccion}
        soluciones.push(comp)
   } else if (d < min_dist && c1.name!=c2.name) {
    min_dist = d;
    soluciones = [ 
        {
        "restaurante 1": c1.name,
        "restaurante 2": c2.name,
        "direccion 1": c1.direccion,
        "direccion 2": c2.direccion,
        }
    ]
   }
  }
 }
 return {
            "restaurants": soluciones ,
            "dist": Math.sqrt(min_dist),"count": reduced.count
        };
},
 query : { "grades.score" : { "$gte" : 5} },
 out: "rest_mapreduce"
 });