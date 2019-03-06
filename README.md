# Story
--------
#### _Acme Inc started business in 2013 and immediately started onboarding members._  
## Enrollment rules:
*  You must be 18 years or older to have an account at Acme Inc.
*  You must provide valid identifiers (email, zip code, phone number) during enrollment.
*  Members are never removed, just cancelled.

# Issue
--------
A developer wrote a script that accidentally messed up some data and there have been a few bugs over the years that could have caused issues. 

# Task
--------
The goal is to use Apache Spark to identify the data that is out of these bounds. 

# Source schema
--------
```
  create table members (
  id int not null auto_increment,
  first_name varchar(255) not null,
  last_name varchar(255) not null,
  email varchar(255) not null,
  phone int(10) not null,
  status enum('active', 'cancelled') not null,
  zip5 int(5) not null,
  created_at datetime,
  updated_at datetime,
  birth_date date not null);
```
 
# Solution
  
* Install sbt from https://www.scala-sbt.org/1.0/docs/Setup.html
   
* Install Spark from https://spark.apache.org/downloads.html  

* Compile spark application

   ```sbt assembly```

* run spark application

   ```spark-submit target/scala-2.11/rustyhands-assembly-0.1.jar > rusty.log```


