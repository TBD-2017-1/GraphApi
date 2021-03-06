package Neo4jAPI;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import PoliTweetsCL.Core.Misc.Config;
import PoliTweetsCL.Core.Model.Tweet;
import PoliTweetsCL.Core.Model.User;
import java.util.Map;

public class Neo4jClass {

    private Driver driver;
    private Session session;

    //Default config -> username: neo4j - password: root
    public Neo4jClass(){
    }

    public void openConnection(String username, String password){
      this.driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( noe4j, mariobroos ) );
      this.session = this.driver.session();
    }

    public void cleanDatabase(){
        this.session.run("match (a)-[r]->(b) delete r");
        this.session.run("match (n) delete n");
    }

    public void closeConnection(){
        this.session.close();
        this.driver.close();
    }

    private void createNode(String type, String name, String twitterAccount){
        if(type.equals("User")){
          this.session.run( "CREATE (a:"+type+" {account:'@"+twitterAccount+"'})");
        }
        else{
          this.session.run( "CREATE (a:"+type+" {name:'"+name+"', account:'@"+twitterAccount+"'})");
        }
    }

    private void createTweetRelation(String nodoOrigen, String nodoDestino){
        this.session.run("match (a) where a.name='"+ nodoOrigen +"' "
                        + "match (b) where b.name='"+ nodoDestino +"' "
                        + "create (a)-[r:tweet{retweet:0, menciones:0}]->(b)");
    }

    private void addRetweet(String nodoOrigen, String nodoDestino){
        StatementResult result;
        result = this.session.run("match (a)-[r:tweet]->(b)"
                                +"where a.name="+nodoOrigen+" and b.name="+nodoDestino+""
                                +"return r.retweet");
        int retweet = Integer.ParseInt(result.next());
        retweet++;
        this.session.run("match (a)-[r:tweet]->(b)"
                        +"where a.name="+nodoOrigen+" and b.name="+nodoDestino+""
                        +"set r.retweet="+retweet);
    }

    private void addMencion(String nodoOrigen, String nodoDestino){
        StatementResult result;
        result = this.session.run("match (a)-[r:tweet]->(b)"
                                +"where a.name="+nodoOrigen+" and b.name="+nodoDestino+""
                                +"return r.retweet");
        int menciones = Integer.ParseInt(result.next());
        menciones++;
        this.session.run("match (a)-[r:tweet]->(b)"
                        +"where a.name="+nodoOrigen+" and b.name="+nodoDestino+""
                        +"set r.retweet="+menciones);
    }

    private boolean existNode(String type, String name){
        StatementResult result;
        result = this.session.run("match (a:"+type+") where a.name="+name+" return a");
        if(result.hasNext()){
          return true;
        }
        return false;
    }

    private boolean existRelation(String origin, String name){
        StatementResult result;
        result = this.session.run("match (a:"+type+") where a.name="+name+" return a");
        if(result.hasNext()){
          return true;
        }
        return false;
    }

    //Este procedimiento debe realizarse 3 veces, para conglomerados, partidos, y politicos.
    public void mapDatabase(Tweet[] tweets, Map<String, String> users, String entidad){
        StatementResult result;

        for (Map.Entry<String, String> entry : users.entrySet()){
          this.createNode(entidad, entry.getKey(), entry.getValue());
        }

        for(Tweet t : tweets){
          if(t.getRetweetedStatus() != null){
            String twitter = t.getUser().getScreenName();
            String retweeted = t.getRetweetedStatus().getUser().getScreenName();
            if(Map.containsValue(retweeted)){
              if(!existNode("User", twitter)){
                this.createNode("User", twitter, twitter);
              }
              this.createRelation("User", twitter, entidad, retweeted);
            }
          }
        }
    }

    public static void main(String[] args) {

        Neo4jClass n4j = new Neo4jClass();
        n4j.openConnection("neo4j", "root");
        n4j.cleanDatabase();
        n4j.mapDatabase();
        n4j.closeConnection();
        /*Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "mariobroos" ) );
        Session session = driver.session();

        session.run("match (a)-[r]->(b) delete r");
        session.run("match (n) delete n");

        session.run( "CREATE (a:Person {name:'Arthur', title:'King'})");
        session.run( "CREATE (a:Person {name:'Lancelot', title:'Sir'})");
        session.run( "CREATE (a:Person {name:'Merlin', title:'Wizard'})");


        StatementResult result = session.run( "MATCH (a:Person) return a.name as name, a.title as title");
        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "title" ).asString() + " " + record.get("name").asString() );
        }


        session.run("match (a:Person) where a.name='Lancelot' "
                + "  match (b:Person) where b.name='Arthur' "
                + "  create (a)-[r:Loyal {reason:'He loves Arthur in secret :3'}]->(b)");


        session.run("match (a:Person) where a.name='Merlin'"
                + "match (b:Person) where b.name='Arthur'"
                + "create (a)-[r:Advise]->(b)");

        session.run("create (a:Person {name:'Guinevere',title:'Lady'})");

        session.run("match (a:Person) where a.name='Arthur'"
                + "match (b:Person) where b.name='Guinevere'"
                + "create (a)-[r:Married {since:'13 Century'}]->(b)");

        session.run("create (a:Person {name:'Percival',title:'Sir'})");
        session.run("create (a:Person {name:'Blanchefleur',title:'Lady'})");

        session.run("match (a:Person) where a.name='Percival'"
                + "match (b:Person) where b.name='Blanchefleur'"
                + "create (a)-[r:Married]->(b)");

        session.run("match (a:Person) where a.name='Percival'"
                + "match (b:Person) where b.name='Arthur'"
                + "create (a)-[r:Loyal]->(b)");

        session.run("match (a:Person) where a.name='Lancelot' "
                + "  match (b:Person) where b.name='Percival' "
                + "  create (a)-[r:Fellow]->(b)");

        result = session.run( "MATCH (a:Person) where a.name='Lancelot' match (a)-[r]->(b:Person) return b.name as name, b.title as title");
        //result = session.run( "MATCH (a:Person) where a.name='Lancelot' match (a)-[r:]->(b:Person) return b.name as name, b.title as title");

        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "title" ).asString() + " " + record.get("name").asString() );
            StatementResult result2 = session.run( "MATCH (a:Person) where a.name='"+record.get("name").asString()+"' match(a)-[r]->(b) return b.name as name, b.title as title");
            while ( result2.hasNext() )
            {
                record = result2.next();
                System.out.println(record.get( "title" ).asString() + " " + record.get("name").asString() );
            }
        }

        String[] names = new String[2];
        names[0] = "Galahad";
        names[1] = "Bors";

        for (String name:names)
        {
            session.run("create (a:Person {name:'"+name+"',title:'Sir'})");
            session.run("match (a:Person) where a.name='Lancelot'"
                    + "match (b:Person) where b.name='"+name+"'"
                    + "create (a)-[r:Fellow]->(b)");
            session.run("match (a:Person) where a.name='Arthur'"
                    + "match (b:Person) where b.name='"+name+"'"
                    + "create (a)-[r:Loyal]->(b)");
        }


        session.close();
        driver.close();*/
    }
}
