select t_movie.movieid
from t_rating
    join t_movie
        on t_movie.movieid = t_rating.movieid
    join (
            select t_rating.movieid, t_rating.rate
            from t_rating
                join (
                    select t_user.userid,
                           count(*) as total
                    from t_user
                        join t_rating
                            on t_rating.userid = t_user.userid
                    where t_user.sex = 'F'
                    group by t_user.userid
                    order by total desc
                    limit 1
                ) as u
                    on u.userid = t_rating.userid
            order by t_rating.rate desc
            limit 10
    ) as m
        on m.movieid = t_rating.movieid
group by t_movie.movieid;
