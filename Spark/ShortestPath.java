import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;
import java.util.Map;

public class ShortestPath {
 
    public static void main(String[] args) {

		// start Sparks and read a given input file
		String inputFile = args[0];
		SparkConf conf = new SparkConf( ).setAppName( "BFS-based Shortest Path Search" );
		JavaSparkContext jsc = new JavaSparkContext( conf );
		JavaRDD<String> lines = jsc.textFile( inputFile );

		// now start a timer
		long startTime = System.currentTimeMillis();

		JavaPairRDD<String, Data> network = lines.mapToPair(line-> {
			String[] v = line.split("=");
			String[] connections = v[1].split(";");
			List<Tuple2<String,Integer>> neighbors = new ArrayList<>();
			for (String connection : connections) {
				String[] edge = connection.split(",");
				neighbors.add(new Tuple2<String,Integer>(edge[0],Integer.parseInt(edge[1])));
			}
			String status = "INACTIVE";
			//set the status of startig node as active
			if(v[0].equals(args[1])){
				status = "ACTIVE";
			}
			Tuple2<String, Data> entry = new Tuple2<>(v[0], new Data(neighbors, 0, null, status));
			return entry;
		});

		Boolean isVertexActive = true;

		//Verifying network formation
		Map<String, Data> stringDataMap = network.collectAsMap();
//		for(Map.Entry<String, Data> entry :stringDataMap.entrySet()){
//			System.out.println("Vertex: "+entry.getKey());
//			entry.getValue().neighbors.forEach(neighbour->{
//				System.out.print(neighbour._1+" ");
//			});
//			System.out.println();
//		}

		while (isVertexActive) {
//			Map<String, Data> stringDataMap = network.collectAsMap();
			//Verifying network formation
//			for(Map.Entry<String, Data> entry :stringDataMap.entrySet()){
//				System.out.println("Vertex: "+entry.getKey());
//				entry.getValue().neighbors.forEach(neighbour->{
//					System.out.print(neighbour._1+" ");
//				});
//				System.out.println();
//			}
			JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair( vertex -> {
				// If a vertex is “ACTIVE”, create Tuple2( neighbor, new Data( … ) ) for
				// each neighbor where Data should include a new distance to this neighbor.
				// Add each Tuple2 to a list. Don’t forget this vertex itself back to the
				// list. Return all the list items.
				List<Tuple2<String,Data>> vertices = new ArrayList<>();
				if(vertex._2.status.equals("ACTIVE")){
					if(!vertex._1.equals(args[2])){
						if(vertex._2.neighbors != null && vertex._2.neighbors.size() > 0 ){
							for(Tuple2<String, Integer> neighbour : vertex._2.neighbors){
								Data neighbour_node = stringDataMap.get(neighbour._1);
								vertices.add(new Tuple2<>(neighbour._1,new Data(neighbour_node.neighbors,vertex._2.distance+neighbour._2,neighbour_node.prev,"INACTIVE")));
							}
						}
					}
//					vertices.add(vertex);
				}
				vertices.add(new Tuple2<>(vertex._1,new Data(vertex._2.neighbors,vertex._2.distance,vertex._2.prev,"INACTIVE")));
				return vertices.iterator();
			});

			network = propagatedNetwork.reduceByKey((k1, k2) -> {
				// For each key, (i.e., each vertex), find the shortest distance and
				// update this vertex’ Data attribute.
				if(k1.distance == 0 ){
					return k2;
				}else if(k2.distance == 0){
					return k1;
				}else if (k1.distance < k2.distance ) {
					return k1;
				} else {
					return k2;
				}
			});

//			for(Map.Entry<String, Data> entry :network_Copy.collectAsMap().entrySet()){
//				System.out.println("Vertex: "+entry.getKey()+" distance so far "+ entry.getValue().distance);
//			}

			network = network.mapValues( value -> {
			// If a vertex’ new distance is shorter than prev, activate this vertex
			// status and replace prev with the new distance.
				if(value.distance !=0 && (value.prev == null || value.prev > value.distance) ){
					value.status = "ACTIVE";
					value.prev = value.distance;
				}
				return value;
			} );

			//check if any vertex is active
			JavaPairRDD<String, Data> activeNodes = network.filter(node -> node._2.status.equals("ACTIVE"));
			isVertexActive = (activeNodes.count() == 0l )?false:true;
		}

		long endtime = System.currentTimeMillis();
		System.out.println( "execution time = " + (endtime-startTime));
		List<Data> destination = network.lookup(args[2]);
		Integer distance = destination.iterator().next().distance;

//		 print shortest distance
		System.out.println( "from "+args[1]+" to "+args[2]+" shortest distance is = " + distance );
    }
 
}
