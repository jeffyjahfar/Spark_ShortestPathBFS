import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;
import java.util.Map;

public class TriangleCounting {
 
    public static void main(String[] args) {

		// start Sparks and read a given input file
		String inputFile = args[0];
		SparkConf conf = new SparkConf( ).setAppName( "TriangleCounting" );
		JavaSparkContext jsc = new JavaSparkContext( conf );
		JavaRDD<String> lines = jsc.textFile( inputFile );

		// now start a timer
		long startTime = System.currentTimeMillis();

		// creation of network from graphs input file
		JavaPairRDD<String, TriData> network = lines.mapToPair(line-> {
			String[] v = line.split("=");
			String[] connections = v[1].split(";");
			List<Tuple2<String,Integer>> neighbors = new ArrayList<>();
			for (String connection : connections) {
				String[] edge = connection.split(",");
				neighbors.add(new Tuple2<String,Integer>(edge[0],Integer.parseInt(edge[1])));
			}
			//set the status of ALL nodeS as active
			String status = "ACTIVE";
			//initialize each vertex with data as neighbour, current path as the vertex name, triangle count as 0 and initial path length as 0
			Tuple2<String, TriData> entry = new Tuple2<>(v[0], new TriData(neighbors, 0, v[0], status,0));
			return entry;
		});

		Boolean isVertexActive = true;

		//Verifying network formation
		Map<String, TriData> stringTriDataMap = network.collectAsMap();
//		for(Map.Entry<String, TriData> entry :stringTriDataMap.entrySet()){
//			System.out.println("Vertex: "+entry.getKey());
//			entry.getValue().neighbors.forEach(neighbour->{
//				System.out.print(neighbour._1+" ");
//			});
//			System.out.println();
//		}
		int edge_traversed = 0;
		while (edge_traversed<2) {

			//Traverse twice to travel 2 edges of the triangle such that movement is from lower vertex to higher vertex only
			JavaPairRDD<String, TriData> propagatedNetwork = network.flatMapToPair( vertex -> {
				List<Tuple2<String,TriData>> vertices = new ArrayList<>();
				if(vertex._2.status.equals("ACTIVE")){
						if(vertex._2.neighbors != null && vertex._2.neighbors.size() > 0 ){
							for(Tuple2<String, Integer> neighbour : vertex._2.neighbors){
								//if neighbour vertex is greater than current vertex value,
								// set the neighbour as active, add current vertex to path and increment path length.
								if(Integer.parseInt(neighbour._1) > Integer.parseInt(vertex._1)){
									TriData neighbour_node = stringTriDataMap.get(neighbour._1);
									Integer pathlength = vertex._2.countPath+1;
									vertices.add(new Tuple2<>(neighbour._1,new TriData(neighbour_node.neighbors,pathlength,vertex._2.path+neighbour._1,"ACTIVE",0)));
								}
							}
						}
				}
				//make the current vertex inactive in the propagated network
				vertices.add(new Tuple2<>(vertex._1,new TriData(vertex._2.neighbors,vertex._2.countPath,vertex._2.path,"INACTIVE",vertex._2.numTriangles)));
				return vertices.iterator();
			});
			network = propagatedNetwork;
			edge_traversed++;
		}

		//Traverse to lower vertex to complete the triangle
		JavaPairRDD<String, TriData> propagatedNetwork = network.flatMapToPair( vertex -> {
			List<Tuple2<String,TriData>> vertices = new ArrayList<>();
			if(vertex._2.status.equals("ACTIVE")){
				if(vertex._2.neighbors != null && vertex._2.neighbors.size() > 0 ){
					for(Tuple2<String, Integer> neighbour : vertex._2.neighbors){
						if(Integer.parseInt(neighbour._1) < Integer.parseInt(vertex._1)){
							TriData neighbour_node = stringTriDataMap.get(neighbour._1);
							Integer pathlength = vertex._2.countPath+1;
							String curr_path = vertex._2.path + neighbour._1;
							Integer numTriangle = 0;
							//check if the traversal formed a triangle by checking path length to be 4 and and first and last vertex to be same
							if(pathlength ==3 && curr_path.charAt(0)==vertex._1.charAt(0)){
								numTriangle =1;
							}
							vertices.add(new Tuple2<>(neighbour._1,new TriData(neighbour_node.neighbors,pathlength,curr_path,"ACTIVE",numTriangle)));
						}
					}
				}
			}
			vertices.add(new Tuple2<>(vertex._1,new TriData(vertex._2.neighbors,vertex._2.countPath,vertex._2.path,"INACTIVE",vertex._2.numTriangles)));
			return vertices.iterator();
		});
		//adding all the traingles by key
		network = propagatedNetwork.reduceByKey( ( k1, k2 ) ->{
			k1.numTriangles = k1.numTriangles+k2.numTriangles;
			return k1;
		});

		//Total triangles in the entire graph
		Integer totalTriangles=0;
		List<Tuple2<String, TriData>> finalnetwork = network.collect();
		for (Tuple2<String,TriData> tuple : finalnetwork) {
			totalTriangles+=tuple._2.numTriangles;
		}

		Double triangleCounter = network.mapToDouble(stringTriDataTuple2 -> {
			return stringTriDataTuple2._2.numTriangles;
		}).sum();

		System.out.println("The number of triangles is "+triangleCounter);


		long endtime = System.currentTimeMillis();
		System.out.println( "execution time = " + (endtime-startTime));
		List<TriData> destination = network.lookup(args[2]);
		Integer distance = destination.iterator().next().distance;

//		 print shortest distance
		System.out.println( "from "+args[1]+" to "+args[2]+" shortest distance is = " + distance );
    }
 
}
