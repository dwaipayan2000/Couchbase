package assign;

import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Scanner;
import java.util.UUID;

public class CouchbaseDatabaseClient {

    private static CouchbaseEnvironment env;
    private static Cluster cbCluster;
    private static Bucket bucket;
    private static LocalDateTime getDandT(){
        LocalDateTime l;
        l=LocalDateTime.now();
        return l;
    }

    private static void init() {
        env = DefaultCouchbaseEnvironment.builder().connectTimeout(10000).retryStrategy(FailFastRetryStrategy.INSTANCE)
                .build();
        cbCluster = CouchbaseCluster.create(env, Arrays.asList("127.0.0.1"));
        cbCluster.authenticate("Dwaipayan", "dcig2000doncouchbase");
        bucket = cbCluster.openBucket("Training");
    }

    private static void shutdown() {
        bucket.close();
        cbCluster.disconnect();
    }

    private static JsonDocument  createDocument(String docId,String str[]) {
        Scanner sc=new Scanner(System.in);
        JsonObject user = JsonObject.create().put("id", docId).put("Name", str[2]).put("emailid",str[0].toLowerCase()).put("pass",str[1]).put("cdateandtime",(getDandT().toString())).putNull("ldateandtime");

        JsonDocument document = JsonDocument.create(docId, user);
        return document;
    }
    private static JsonDocument  createDocument(String str) {
        String docId = UUID.randomUUID().toString();
        Scanner sc=new Scanner(System.in);
        System.out.println("please fill the following details to Register :-");
        System.out.println("Please Enter your Name for which your email id is:- " + str);
        String name=sc.nextLine();

        System.out.println("Please Enter your password for which your email id is:- "+ str);
        String pass=sc.nextLine();
        JsonObject user = JsonObject.create().put("id", docId).put("Name", name).put("emailid",str).put("pass",pass).put("cdateandtime",(getDandT().toString())).putNull("ldateandtime");

        JsonDocument document = JsonDocument.create(docId, user);
        if(document==null){
            System.out.println("Registration Failed.. Try again");
        }else{
            System.out.println("Successfully Registered.. You can login now. Thank You");
        }
        return document;
    }

    private static void searchingemail(String str) {
        JsonObject pVal = JsonObject.create().put("emailid", str);
        String query = "SELECT id FROM `Training` " + "WHERE emailid = $emailid";
        final boolean[] flag = {true};
        bucket.query(N1qlQuery.parameterized(query, pVal)).forEach(row -> {
            JsonObject customerData = row.value();
            if(customerData!=null){
                System.out.println("You are an existing account holder. Try login.....");
                flag[0] =false;
            }
        });
        if(flag[0]==true){
            System.out.println("you are a new customer");
            bucket.insert(createDocument(str));
        }

    }
    private static void printdetails(String str){
        bucket.upsert(updatelogintime(str));
        JsonDocument resultantDocument = bucket.get(str);
        System.out.println("Name :- "+resultantDocument.content().getString("Name"));
        System.out.println("Email Id :- "+resultantDocument.content().getString("emailid"));
        System.out.println("Account Created Date and Time :- "+resultantDocument.content().getString("cdateandtime"));
        System.out.println("Last Login to Account Date and Time :- "+resultantDocument.content().getString("ldateandtime"));


    }
    private  static JsonDocument  updatelogintime(String str){
        JsonDocument resultantDocument = bucket.get(str);

        JsonObject user = resultantDocument.content().removeKey("ldateandtime").put("ldateandtime", LocalDateTime.now().toString());
        JsonDocument document = JsonDocument.create(str, user);
        return document;
    }

    private static void searchingemailforlogin(String str) {
        Scanner sc=new Scanner(System.in);
        JsonObject pVal = JsonObject.create().put("emailid", str);
        String query = "SELECT pass,id FROM `Training` " + "WHERE emailid = $emailid";
        final boolean[] flag = {true};
        bucket.query(N1qlQuery.parameterized(query, pVal)).forEach(row -> {
            JsonObject customerData = row.value();
            if(customerData!=null){
                String id=customerData.getString("id");
                String s=customerData.getString("pass");
                String userpass;
                boolean f=true;
                while(f){
                    System.out.println("Please Enter your password for the emailid :- "+ str);
                    userpass=sc.nextLine();
                    if(s.equals(userpass)){
                        printdetails(id);
                        f=false;
                    }else {
                        System.out.println("Wrong Password!!!.. If You Want To Try Again Please Enter \n1. For Password Retry \n2. For Exit Login");
                        int x;
                        x=sc.nextInt();
                        sc.nextLine();
                        switch (x){
                            case 1:
                                System.out.println("Please Enter Your Password Again.");
                                break;
                            case 2:
                                f=false;
                                break;
                        }
                    }
                }
                flag[0] =false;
            }
        });
        if(flag[0]==true){
            System.out.println("you are a new customer");
            bucket.insert(createDocument(str));
        }

    }
    private  static void bulkupload(File file,Scanner sc) throws FileNotFoundException {
        sc=new Scanner(file);
        while(sc.hasNextLine()){
            String docId = UUID.randomUUID().toString();
            String str1[]=sc.nextLine().split("\\s+");
            bucket.insert(createDocument(docId,str1));
        }
    }
    private static void regonline(String str){
        searchingemail(str);
    }
    private static void login(String str){
        searchingemailforlogin(str);
    }

    public static void main(String[] args) throws InterruptedException {
        Scanner sc=new Scanner(System.in);
        init();
        boolean flag=true;
        while(flag){
            System.out.println("Enter --> ");
            System.out.println("1) Bulk Uploading");
            System.out.println("2) Register Online");
            System.out.println("3) Login");
            System.out.println("4) Shutdown");
            int x=sc.nextInt();
            switch (x){
                case 1 :// for bulk uploading
                    String str;
                    System.out.println("Please Enter the path of the file for Bulk Uploading");
                    sc.nextLine();
                    str=sc.nextLine();
                    File file=new File(str);
                    try{
                        bulkupload(file,sc);
                    }catch (FileNotFoundException e){
                        System.out.println("Bulk Uploading failure!");
                        e.printStackTrace();
                    }
                    break;
                case 2 ://for registering
                    System.out.println("Please Enter Emailid to check that you are existing account holder or not");
                    sc.nextLine();
                    str=sc.nextLine();
                    str=str.toLowerCase();
                    regonline(str);
                    break;
                case 3 :// for login
                    System.out.println("Please Enter Emailid to login :-");
                    sc.nextLine();
                    str=sc.nextLine();
                    str=str.toLowerCase();
                    login(str);
                    break;
                case 4://for shutdown
                    flag=false;
                    shutdown();
                    break;
                default:
                    System.out.println("Please Enter a Valid Choice");
            }
        }
    }

}