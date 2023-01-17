package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession

object objSQLScenarios {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("First").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;
			
      val df  = spark.read.option("header","true").csv("file:///D:/My Learning/Big Data/SQL Scenarios/df.csv")
      val df1 = spark.read.option("header","true").csv("file:///D:/My Learning/Big Data/SQL Scenarios/df1.csv")
      val cust = spark.read.option("header","true").csv("file:///D:/My Learning/Big Data/SQL Scenarios/cust.csv")
      val prod = spark.read.option("header","true").csv("file:///D:/My Learning/Big Data/SQL Scenarios/prod.csv")
      					
      /*df.show()
      df1.show()
      cust.show()
      prod.show()*/

      df.createOrReplaceTempView("df")
      df1.createOrReplaceTempView("df1")
      cust.createOrReplaceTempView("cust")
      prod.createOrReplaceTempView("prod")

      
      println("========================= Select All Tables =========================");
      spark.sql("select * from df order by id").show()
      spark.sql("select * from df1  order by id").show()
      spark.sql("select * from cust order by id").show()
      spark.sql("select * from prod  order by id").show()
      
      
      println("========================= Select two columns =========================");
      spark.sql("select id,tdate from df  order by id").show()
      
      println("========================= Select column with category filter =========================");
      spark.sql("select id,tdate,category from df where category='Exercise' order by id").show()

      println("========================= Multi Column filter =========================");	    
	    spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash'").show()
	    
      println("========================= Multi Value filter =========================");
	    spark.sql("select * from df   where category in ('Exercise','Gymnastics')").show()
	    
	    println("========================= Like Filter =========================");
	    spark.sql("select * from df   where product like ('%Gymnastics%')").show()
	    
	    println("========================= Not Filters =========================");
	    spark.sql("select * from df   where category != 'Exercise'").show()
	    
	    println("========================= Not In Filters =========================");
	    spark.sql("select * from df   where category not in ('Exercise','Gymnastics')").show()
	    
	    println("========================= Null Filters =========================");
	    spark.sql("select * from df   where product is null").show()
	    
	    println("========================= Max Function =========================");
	    spark.sql("select max(id) from df   ").show()
	    
	    println("========================= Min Function =========================");
	    spark.sql("select min(id) from df   ").show()
	    
	    println("========================= Count =========================");
	    spark.sql("select count(1) from df   ").show()
	    
	    println("========================= Condition statement =========================");
	    spark.sql("select *,case when spendby='cash' then 1 else 0 end  as status from df  ").show()
	    
	    println("========================= Concat data =========================");
	    spark.sql("select concat(id,'-',category) as concat  from df  ").show()
	    
	    println("========================= Concat_ws data =========================");
	    spark.sql("select concat_ws('-',id,category,product) as concat   from df    ").show()
	    
	    println("========================= Lower Case data =========================");
	    spark.sql("select category,lower(category) as lower  from df ").show()
	    
	    println("========================= Ceil data =========================");
	    spark.sql("select amount,ceil(amount) from df").show()
	    
	    println("========================= Round the data =========================");
	    spark.sql("select amount,round(amount) from df").show()
	    
	    println("========================= Replace Nulls =========================");
	    spark.sql("select coalesce(product,'NA') from df").show()
	    
	    println("========================= Trim the space =========================");
      spark.sql("select trim(product) from df").show()
      
      println("========================= Distinct of columns =========================");
      spark.sql("select distinct category,spendby from df").show()
      
      println("========================= Substring with Trim =========================");
      spark.sql("select substring(trim(product),1,10) from df").show()
      
      println("========================= Substring/Split operation =========================");
      spark.sql("select category, SUBSTRING_INDEX(category,' ',1) as spl from df").show()
      
      println("========================= Union all =========================");
      spark.sql("select * from df union all select * from df1").show()
      
      println("========================= Union =========================");
      spark.sql("select * from df union select * from df1 order by id").show()
      
      println("========================= Aggregate Sum =========================");
      spark.sql("select category, sum(amount) as total from df group by category").show()
      
      println("========================= Aggregate sum with two columns =========================");
      spark.sql("select category,spendby,sum(amount)  as total from df group by category,spendby").show()
      
      println("========================= Aggregate Count =========================");
      spark.sql("select category,spendby,sum(amount) As total,count(amount)  as cnt from df group by category,spendby").show()
      
      println("========================= Aggregate Max  =========================");
      spark.sql("select category, max(amount) as max from df group by category").show()
      
      println("========================= Aggregate Max with Order =========================");
      spark.sql("select category, max(amount) as max from df group by category order by category").show()
      
      println("========================= Aggregate with Order Descending =========================");
      spark.sql("select category, max(amount) as max from df group by category order by category desc").show()
      
      println("========================= Window Row Number =========================");
      spark.sql("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()
      
      println("========================= Window Dense_rank Number =========================");
      spark.sql("SELECT category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()
      
      println("========================= Window rank Number =========================");
      spark.sql("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()
      
      println("========================= Window Lead function =========================");
      spark.sql("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()
      
      println("========================= Window lag  function =========================");      
      spark.sql("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()
      
      println("========================= Having function =========================");
      spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()
      
      println("========================= Inner Join =========================");
      spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()
      
      println("========================= Left Join =========================");
      spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()
      
      println("========================= Right Join =========================");
      spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()
      
      println("========================= Full Join =========================");
      spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()
      
      println("========================= left anti Join =========================");
      spark.sql("select a.id,a.name from cust a LEFT ANTI JOIN  prod b on a.id=b.id").show()
      
      println("========================= Left semi join =========================");
      spark.sql("select a.id,a.name from cust a LEFT SEMI JOIN  prod b on a.id=b.id").show()
      
      println("========================= Date format =========================");
      spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()
      
      println("========================= Sub query =========================");
      spark.sql("""
            select sum(amount) as total , con_date from(
            select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df) 
            group by con_date
      """).show()
      
      println("========================= collect_list =========================");      
      spark.sql("select category,collect_list(spendby) as col_spend from df group by category").show()
      
      println("========================= collect_set =========================");
      spark.sql("select category,collect_set(spendby) as col_spend from df group by category").show()
      
      println("========================= explode =========================");
      spark.sql("select category,explode(col_spend) as ex_spend from (select category,collect_set(spendby) as col_spend from df group by category)").show()
      
      println("========================= explode outer =========================");
      spark.sql("select category,explode_outer(col_spend) as ex_spend from (select category,collect_set(spendby) as col_spend from df group by category)").show()
	}
}
