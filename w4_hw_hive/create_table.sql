create table t_movie
(
    MovieID   int,
    MovieName string,
    MovieType string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::");

create table t_rating
(
    UserID  int,
    MovieID int,
    Rate    int,
    Times   int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::");

create table t_user
(
    UserID     int,
    Sex        string,
    Age        int,
    Occupation int,
    Zipcode    int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::");

LOAD DATA LOCAL INPATH '/data/hive/movies.dat' OVERWRITE INTO TABLE t_movie;
LOAD DATA LOCAL INPATH '/data/hive/ratings.dat' OVERWRITE INTO TABLE t_rating;
LOAD DATA LOCAL INPATH '/data/hive/users.dat' OVERWRITE INTO TABLE t_user;

-- docker run -it -v /Users/leochen/hive:/data/hive nagasuga/docker-hive /bin/bash -c 'cd /usr/local/hive && ./bin/hive'
