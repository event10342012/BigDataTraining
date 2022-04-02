select t_user.sex,
       t_movie.moviename  as name,
       avg(t_rating.rate) as avgrate,
       count(*)           as total
from t_user
         join t_rating
              on t_user.userid = t_rating.userid
         join t_movie
              on t_rating.movieid = t_movie.movieid
where t_user.sex = 'M'
group by t_user.sex, t_movie.moviename
having count(*) > 50
order by avgrate desc
limit 10;