CREATE TABLE rank (id int, rank int, year int, gender 
	char(1), count int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( START (2006) END (2016) EVERY (1), 
	  DEFAULT PARTITION extra ); 


CREATE OR REPLACE FUNCTION random_between(low INT ,high INT) 
   RETURNS INT AS
$$
BEGIN
	   RETURN floor(random()* (high-low + 1) + low);
END;
$$ language 'plpgsql' STRICT;

insert into rank
select i, i, random_between(2005, 2017), 'g', i
from generate_series(1, 100000)i;
