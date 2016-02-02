
DEFINE computeMatrixNorms(cData,sqrt_gamma) RETURNS Matrix_Norms {
    cData_grp = GROUP $cData BY MovieID;
    -- On calcule la norme et le gamma sur la norme
    $Matrix_Norms = FOREACH cData_grp {
        tmp_out = FOREACH $cData GENERATE Rating*Rating;
        out = SUM(tmp_out);
        GENERATE group as MovieID, SQRT(out) as Norm, ($sqrt_gamma.$0/SQRT(out)>1?1:$sqrt_gamma.$0/SQRT(out)) as Prob_j,RANDOM() as Rand_j,RANDOM() as Rand_k;
    }
}

cData =  LOAD '$CONTAINER/imdd/ratings_mean_short.csv'
        using PigStorage (',')
        AS (UserID:int, MovieID:int, Rating:double) ;

-- On calcul le gamma
users = GROUP cData all ;
total= FOREACH users GENERATE MAX($1.UserID) as m, MAX($1.MovieID) as n;
sqrt_gamma = FOREACH total GENERATE SQRT(4*LOG(n)/0.5) as a;
        
        
-- On calcule la norme et le gamma sur la norme
Matrix_Norms = computeMatrixNorms(cData,sqrt_gamma);

-- On ajoute la colonne Norm et probabilite dans cData
C = JOIN cData BY MovieID,Matrix_Norms BY MovieID;
D = FOREACH C GENERATE cData::UserID as UserID_f,cData::MovieID as MovieID_f,cData::Rating as Rating_f,
                        Matrix_Norms::Norm as Norm_f,Matrix_Norms::Prob_j as Prob_j_f,Rand_j as Rand_j,Rand_k as Rand_k;

Matrix_data = GROUP D BY UserID_f;
FF = FOREACH Matrix_data GENERATE group as UID, FLATTEN(D.MovieID_f) as MV1;
-- Ajout des informations de MV1
FFF = JOIN FF BY (UID,MV1), D BY (UserID_f,MovieID_f); 
-- Condition de validite premier IF
FFD = FILTER FFF BY RANDOM()<Prob_j_f;
-- Ajout de la seconde loop
GG = JOIN FFD BY UID, D BY UserID_f;
-- Cleaning du tableau
GGG = FOREACH GG GENERATE FFD::FF::UID as UserID,FFD::FF::MV1 as MV_1,FFD::D::Rating_f as Rating_1,FFD::D::Norm_f as Norm_1,
                FFD::D::Prob_j_f as Proba_1,D::MovieID_f as MV_2,D::Rating_f as Rating_2,
                D::Norm_f as Norm_2,D::Prob_j_f as Proba_2,
                D::Rand_j as Rand_j,D::Rand_k as Rand_k;
                                
-- Ajout de la deuxieme boucle
-- Condition de validite second IF
GGD = FILTER GGG BY RANDOM()<Proba_2;
DESCRIBE GGD;
-- Generation des similarites
HH = FOREACH GGD{
    val = Rating_1*Rating_2/(((sqrt_gamma.$0>Norm_1)?Norm_1:sqrt_gamma.$0)*((sqrt_gamma.$0>Norm_2)?Norm_2:sqrt_gamma.$0));
    GENERATE MV_1,MV_2,val;
}

STORE HH INTO '$CONTAINER/$PSEUDO/dom/matrix_all2.txt' USING PigStorage(',');