select t_user.age, avg(t_rating.rate) as agvrate
from t_user
         join t_rating
              on t_user.userid = t_rating.userid
         join t_movie
              on t_movie.movieid = t_rating.movieid
where t_movie.movieid = '2116'
group by t_user.age;
