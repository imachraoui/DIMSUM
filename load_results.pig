

cData =  LOAD '$CONTAINER/$PSEUDO/dom/similarities.txt'
        using PigStorage (',')
        AS (MovieID1:int, MovieID2:int, sim:double) ;

filtered = FILTER cData BY MovieID1 == $MvID ;
ordered = ORDER filtered BY sim DESC;
ordered_limit = LIMIT ordered $size;
movies = FOREACH ordered_limit GENERATE MovieID2;
STORE movies INTO '$CONTAINER/imdd/dom/recom.txt' USING PigStorage(',');