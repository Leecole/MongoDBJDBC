package com.oracle.MongoDB;


import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.sun.org.apache.xalan.internal.xsltc.dom.FilterIterator;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBJDBC {

    public static void main( String args[] ){
        MongoClient mongoClient;        //mongo客户端
        MongoDatabase mongoDatabase;    //mongo数据库
        MongoCollection<Document> mongoCollection;//mongo集合（表）
        Document document;              //mongo列

        FindIterable<Document> findIterable;    //查询迭代
        MongoCursor<Document> mongoCursor;      //游标


        try{

/**********************************连接mongo数据库*******************************************************************/
            // 1:连接到 mongodb 服务
            mongoClient = new MongoClient( "localhost" , 27017 );

            // 2:连接到数据库 admin
             mongoDatabase = mongoClient.getDatabase("admin");
            System.out.println("Connect to database successfully");

            //3:创建集合（创建表）,只允许同一个表创建一次，如需运行两次该demo,就必须第二次注释掉该创建表方法
//            mongoDatabase.createCollection("mongoTest");
//            System.out.println("create Collections successfull!!!");

            //4:获取集合(获取表)
            mongoCollection=mongoDatabase.getCollection("admin");
            System.out.println("集合选择成功");

/*****************************5：插入文档（mongo数据库插入数据） *****************************************************/

            //如果不想重复插入数据，则需要注释掉插入方法
            /**
             * 1. 创建文档 org.bson.Document 参数为key-value的格式
             * 2. 创建文档集合List<Document>
             * 3. 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用 mongoCollection.insertOne(Document)
             * */
            document=new Document("title","test1").append("description","mongo").
                                                    append("like",88).
                                                    append("By","down");

            //定义一个数据库集合，将文档存入集合中
            List<Document> documents=new ArrayList<Document>();
            //将单个文档添加到文档集合中
            documents.add(document);
            //将文档集合插入到集合（表）中
            mongoCollection.insertMany(documents);
            System.out.println("数据（文档）插入成功！！！");

/*****************************6:查询：检索所有文档 *********************************************************/

            System.out.println("开始查询......");
            //获取迭代器FindIterable<Document>
             findIterable = mongoCollection.find();
            //获取游标MongoCursor<Document>
            mongoCursor = findIterable.iterator();

            //通过游标遍历检索出的文档集合:迭代器遍历
            while(mongoCursor.hasNext()){
                System.out.println("***"+mongoCursor.next()+"***");
            }

/*****************************7:更新：更新文档 *********************************************************/

            System.out.println("开始更新.....");
            //将文档中like=100的文档修改为like=200
            mongoCollection.updateMany(Filters.eq("like",100),new Document("$set",new Document("like",200)));

            //检索查看结果：迭代器遍历
            findIterable=mongoCollection.find();
            mongoCursor=findIterable.iterator();

            //如果修改成功，则将修改列的全部遍历出来
            while(mongoCursor.hasNext()){
                System.out.println("***"+mongoCursor.next()+"****");
            }

/*****************************8:删除：删除指定的文档 *********************************************************/
            System.out.println("开始删除操作.....");
            //删除符合条件的第一个文档
            mongoCollection.deleteOne(Filters.eq("like",20));
            //删除所有符合条件的文档
            mongoCollection.deleteMany(Filters.eq("title","MongoDB"));

            //检索查看结果
            findIterable=mongoCollection.find();
            mongoCursor=findIterable.iterator();

            //遍历：迭代器
            while(mongoCursor.hasNext()){
                System.out.println("删除之后的列******"+mongoCursor.next()+"*****");
            }



        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }
}
