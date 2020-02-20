/*
Class TriData Definition
*/

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**                                                                                                                        
 * Vertex Attributes
 */
public class TriData implements Serializable {
    List<Tuple2<String,Integer>> neighbors; // <neighbor0, weight0>, <neighbor1, weight1>, ...
    String status;                          // "INACTIVE" or "ACTIVE"
    Integer countPath;                       // the distance so far from source to this vertex
    String path;                           // the distance calculated in the previous iteration
    Integer numTriangles;

    public TriData(){
        neighbors = new ArrayList<>();
        status = "INACTIVE";
        countPath = 0;
        path="";
    }

    public TriData(List<Tuple2<String,Integer>> neighbors, Integer countPath, String path, String status, Integer numTriangles ){
        if ( neighbors != null ) {
            this.neighbors = new ArrayList<>( neighbors );
        } else {
            this.neighbors = new ArrayList<>( );
        }
        this.countPath = countPath;
	    this.path = path;
        this.status = status;
        this.numTriangles = numTriangles;
    }
}
