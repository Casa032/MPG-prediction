/*NOM: VINCENT*/
/*PRENOM: Rydch Junior*/


LIBNAME tp "C:\Users\vjuni\Documents\Universit� de Lille\ESM3\S5\Projet_SAS_ESM3\Projet_sas_31";

/*Importation des librairies*/

	/*Auto-mpg-1*/
PROC IMPORT DATAFILE="C:\Users\vjuni\Documents\Universit� de Lille\ESM3\S5\Projet_SAS_ESM3\Projet_sas_31\auto-mpg-1.xlsx" OUT=tp.mpg1
DBMS=xlsx REPLACE;
GETNAMES=YES;
RUN;

	/*Auto-mpg-2*/
PROC IMPORT DATAFILE="C:\Users\vjuni\Documents\Universit� de Lille\ESM3\S5\Projet_SAS_ESM3\Projet_sas_31\auto-mpg-2.xlsx" OUT=tp.mpg2
DBMS=xlsx REPLACE;
GETNAMES=YES;
RUN;

	/*Auto-mpg-3*/
PROC IMPORT DATAFILE="C:\Users\vjuni\Documents\Universit� de Lille\ESM3\S5\Projet_SAS_ESM3\Projet_sas_31\auto-mpg-3.xlsx" OUT=tp.mpg3
DBMS=xlsx REPLACE;
GETNAMES=YES;
RUN;

	/*Auto-mpg-a-predire*/
PROC IMPORT DATAFILE="C:\Users\vjuni\Documents\Universit� de Lille\ESM3\S5\Projet_SAS_ESM3\Projet_sas_31\auto-mpg-a-predire.xlsx" OUT=tp.mpg_a_predire
DBMS=xlsx REPLACE;
GETNAMES=YES;
RUN;

/*Fusion des tables */

	/*Cr�ation auto_mpg_1_2*/
PROC SORT DATA=tp.mpg1; BY identifiant ; RUN;
PROC SORT DATA=tp.mpg2; BY identifiant ; RUN;

DATA tp.auto_mpg_1_2 ; MERGE tp.mpg1 tp.mpg2;
BY identifiant;
RUN;

	/*Cr�ation auto_mpg*/
DATA tp.auto_mpg ; SET tp.auto_mpg_1_2 tp.mpg3;
RUN;

/*Analyse des variables */

PROC CONTENTS DATA=tp.auto_mpg; RUN;

PROC PRINT DATA=tp.auto_mpg; RUN;

	/* Creation de la variable age */
DATA tp.auto_mpg; 
SET tp.auto_mpg;
Age = 1983 - annee_du_modele; 
DROP annee_du_modele;
RUN;

DATA tp.auto_mpg; 
SET tp.auto_mpg;
IF _N_ = 8 THEN DO;
	nom_de_la_voiture="mazda rx3";
END ;
IF _N_ = 82 THEN DO;
	nom_de_la_voiture="toyota corona mark ii (sw)";
END ;
RUN;


/* Statistique descriptive du jeu de donnees initial*/
PROC SORT DATA=tp.auto_mpg; BY origine; RUN;


PROC FREQ DATA=tp.auto_mpg;
TABLES origine/NOCUM;
RUN;

PROC FREQ DATA=tp.auto_mpg;
TABLES nom_de_la_voiture/NOPercent;
BY origine;
RUN;

PROC MEANS DATA=tp.auto_mpg N NMISS MIN MAX RANGE MEAN MEDIAN STD SKEWNESS KURTOSIS;
VAR poids cylindres puissance acceleration deplacement age mpg ;
LABEL age="Age";
RUN;

PROC MEANS DATA=tp.auto_mpg N NMISS MIN MAX RANGE MEAN MEDIAN STD SKEWNESS KURTOSIS;
CLASS origine;
VAR poids cylindres puissance acceleration deplacement age mpg ;
LABEL age="Age";
RUN;



/*Boxplot de la variable d�placement en fonction de l'origine*/
PROC BOXPLOT DATA= tp.auto_mpg; 
PLOT deplacement * origine
/ CAXIS = black CTEXT = black CBOXES = black 
BOXSTYLE = schematic
IDCOLOR = black IDSYMBOL=dot; 
INSET MIN MEAN MAX STDDEV / 
HEADER = 'Overall Statistics' 
POSITION=TM;
INSETGROUP N min max NHIGH NLOW NOUT Q1 Q3/ 
HEADER= 'Extremes par groupe';
RUN;

/*Boxplot de la variable mpg en fonction de l'origine*/

PROC BOXPLOT data=tp.auto_mpg; 
PLOT mpg * origine
/ CAXIS = black CTEXT = black CBOXES = black 
BOXSTYLE = schematic
IDCOLOR = black IDSYMBOL=dot; 
INSET MIN MEAN MAX STDDEV / 
HEADER = 'Overall Statistics' 
POSITION=TM;
INSETGROUP N min max NHIGH NLOW NOUT Q1 Q3 / 
HEADER= 'Extremes par groupe';
RUN;

/*Boxplot de la variable puissance en fonction de l'origine*/

PROC BOXPLOT data=tp.auto_mpg; 
PLOT puissance * origine
/ CAXIS = black CTEXT = black CBOXES = black 
BOXSTYLE = schematic
IDCOLOR = black IDSYMBOL=dot; 
INSET MIN MEAN MAX STDDEV / 
HEADER = 'Overall Statistics' 
POSITION=TM;
INSETGROUP N min max NHIGH NLOW NOUT Q1 Q3 / 
HEADER= 'Extremes par groupe';
RUN;

/*Boxplot de la variable poids en fonction de l'origine*/

PROC BOXPLOT data= tp.auto_mpg; 
PLOT poids * origine
/ CAXIS = black CTEXT = black CBOXES = black 
BOXSTYLE = schematic
IDCOLOR = black IDSYMBOL=dot; 
INSET MIN MEAN MAX STDDEV / 
HEADER = 'Overall Statistics' 
POSITION=TM;
INSETGROUP N min max NHIGH NLOW NOUT Q1 Q3 / 
HEADER= 'Extremes par groupe';
RUN;

/*Boxplot de la variable acceleration en fonction de l'origine*/
PROC BOXPLOT data= tp.auto_mpg; 
PLOT acceleration * origine
/ CAXIS = black CTEXT = black CBOXES = black 
BOXSTYLE = schematic
IDCOLOR = black IDSYMBOL=dot; 
INSET MIN MEAN MAX STDDEV / 
HEADER = 'Overall Statistics' 
POSITION=TM;
INSETGROUP N min max NHIGH NLOW NOUT Q1 Q3 / 
HEADER= 'Extremes par groupe';
RUN;

/*Boxplot de la variable cylindres en fonction de l'origine*/
PROC BOXPLOT data= tp.auto_mpg; 
PLOT cylindres * origine
/ CAXIS = black CTEXT = black CBOXES = black 
BOXSTYLE = schematic
IDCOLOR = black IDSYMBOL=dot; 
INSET MIN MEAN MAX STDDEV / 
HEADER = 'Overall Statistics' 
POSITION=TM;
INSETGROUP N min max NHIGH NLOW NOUT Q1 Q3 / 
HEADER= 'Extremes par groupe';
RUN;

/*Elimination des valeurs abrrerantes*/
/* On �limine les valeurs qui sont dans l�intervalle +]-8;Q1-IQ*1.5]?[Q3+IQ*1.5;+8� */
/*IQ est obtenue par la soustraction du 1er quartile du 3eme quartile: Q3 - Q1 */
DATA tp.auto_mpg_clean; SET tp.auto_mpg;
IF origine=:'Europe' AND deplacement > 159 THEN DELETE;
IF origine=:'Europe' AND mpg>39  THEN DELETE;
IF origine=:'USA' AND mpg>= 38 THEN DELETE;
IF origine=:'Europe' AND acceleration>23.25  THEN DELETE;
IF origine=:'USA' AND acceleration>22  THEN DELETE;
RUN;


/*Traitement des valeurs manquantes*/

PROC MEANS DATA=tp.auto_mpg_clean N NMISS MIN MEAN MAX STDDEV MEDIAN ; 
VAR deplacement puissance acceleration poids;
CLASS origine;
OUTPUT OUT=median_result(drop=_type_ _freq_) median=median_value1-median_value4;
BY origine;
RUN;

DATA tp.mpg_final; MERGE tp.auto_mpg_clean median_result; BY origine;
IF deplacement= . then deplacement = median_value1;
IF puissance= .  then puissance = median_value2;
IF acceleration=. then acceleration = median_value3;
IF poids=.  then poids= median_value4;
DROP median_value1-median_value4;
RUN;


/* Statistique descriptive du jeu de donnees nettoy�*/

PROC FREQ DATA=tp.mpg_final;
TABLES origine/NoCUM;
RUN;

PROC MEANS DATA=tp.mpg_final N NMISS MIN MAX RANGE MEAN MEDIAN STD SKEWNESS KURTOSIS;
CLASS origine;
VAR poids cylindres puissance acceleration deplacement age mpg ;
LABEL age="Age";
RUN;

/*Histogramme de fr�quence*/

	/*Histogramme de mpg par origine*/
PROC UNIVARIATE DATA=tp.mpg_final;
	TITLE "Kilom�trage/gallon par origine";
    CLASS origine;
	VAR mpg;
	HISTOGRAM  /overlay grid vscale=count;
RUN;

	/*Histogramme de d�placement par origine*/
PROC UNIVARIATE DATA=tp.mpg_final;
    CLASS origine;
	VAR deplacement;
	HISTOGRAM  /overlay grid vscale=count;
RUN;

	/*Histogramme de puissance par origine*/
PROC UNIVARIATE DATA=tp.mpg_final;
    CLASS origine;
	VAR puissance;
	HISTOGRAM  /overlay grid vscale=count;
RUN;

	/*Histogramme de poids par origine*/
PROC UNIVARIATE DATA=tp.mpg_final;
    CLASS origine;
	VAR poids;
	HISTOGRAM  /overlay grid vscale=count ;
RUN;

	/*Histogramme de acc�leration par origine*/
PROC UNIVARIATE DATA=tp.mpg_final;
    CLASS origine;
	VAR acceleration;
	HISTOGRAM  /overlay grid vscale=count;
RUN;


/*R�gression lin�aire*/
DATA tp.rlm;
SET tp.mpg_final;
IF origine=:'USA' Then USA=1;
ELSE USA=0;
IF origine=:'Asie' Then Asie=1;
ELSE Asie=0;
IF origine=:'Europe' Then Europe=1;
ELSE Europe=0;
RUN;
QUIT ;


/*Matrice de corr�lation*/
PROC CORR DATA=tp.rlm; 
TITLE 'Corr�lation'; 
VAR deplacement puissance acceleration poids  age cylindres mpg;
RUN ; /* forte corr�lation entre d�placement et (cylindres poids mpg) *//*on doit enlever l'une des variables*/

PROC CORR DATA=tp.rlm; 
TITLE 'Corr�lation'; 
VAR  puissance acceleration mpg age poids cylindres;
RUN ;/* apres avoir enlev� d�placement*/

/* R�gression du mpg sur les variables explicatives*/
PROC REG DATA=tp.rlm;
model mpg= puissance acceleration age poids cylindres Europe USA ;
RUN; /*les variable puissance, acceleration, cylindres sont statistiquement non signifivatives (p-value>0.05) au mod�le*/
QUIT;

/* Regression du mpg sur les variables explicatives restantes */
PROC REG DATA=tp.rlm;
model mpg= age poids Europe USA ;
RUN; 
QUIT;

/* Analyse de la multicolin�arit�*/

PROC REG DATA=tp.rlm corr;
model mpg= age poids Europe USA /vif collinoint;
RUN;/*VIF mesure le d�gre de multicolin�arit� comme VIF pour chaque variable est inf�rieur � 10 on est � un niveau acceptable */
QUIT;


/* Analyse de l'auto-corr�lation*/

PROC REG DATA=tp.rlm corr;
model mpg= age poids Europe USA /dw;
RUN;
QUIT;

PROC AUTOREG DATA=tp.rlm;
   model mpg=age poids Europe USA / dw=4 dwprob;
RUN;
QUIT;
/* La statistique de Durbin-Watson (DW) mesure de la pr�sence d'autocorr�lation, elle varie de 0 � 4.
DW inf�rieur � 2 sugg�re une autocorr�lation positive
Pr < DW et Pr > DW : Des p-values faibles (par exemple, p-value < 0.05) sugg�rent le rejet de l'hypoth�se nulle qui 
est l'abscense d'autocorr�lation*/
/* Le test DW r�v�le la pr�sence d'autocorr�lation pour notre test*/


/* Analyse de l'h�t�rosc�dasticit�*/
PROC REG DATA=tp.rlm corr;
model mpg= age poids Europe USA/spec;
RUN; /*pr�sence d'h�t�rosc�dasticit�*/
QUIT;
/* Le test de White permet de diagnostiquer l'h�t�rosc�dasticit� �ventuelle. L'hypoth�se nulle est que la variance des r�sidus est une constante.*/
/* Avec un p-value de 0.0063, on rejette l'hypoth�se nulle */

/* Analyse de l'influence*/

PROC REG DATA=tp.rlm corr;
model mpg= age poids Europe USA/ r influence;
RUN;
QUIT;

/*  Analyse en fonction du R�sidu studentis�*/
/*Elimination de l'observation 8, identifiant 111*/
/*Avec un r�sidu studentis� sup�rieur a 3 en valeur absolu cette observation est potentiellement influente sur le mod�le, de plus l'identifiant r�prensente
la valeur minimal en kilom�trage par gallon pour la r�gion d'Asie, on choisit de le retirer du mod�le comme valeur atypique*/
/*Elimination de l'observation 54,56,132,288, identifiant 321,328,326,348 respectivement*/
/*On exclut ces observations car elles peuvent etre consid�r�� comme des valeurs ab�rrantes */

DATA tp.rlm ; SET tp.rlm;
IF identifiant=111 THEN DELETE;
IF identifiant=321 THEN DELETE;
IF identifiant=326 THEN DELETE;
IF identifiant=328 THEN DELETE;
IF identifiant=348 THEN DELETE;
RUN;

/* Analyse en fonction de la distance de Cook*/
/* Comme l'objectif final de notre mod�le vise la pr�diction nous pouvons nous permettre de consid�rer un seuil suffisament �lev�
pour la distance de Cook. Ainsi, nous garderons certaines observations dont la distance est sup�rieur � 0.02 consid�r�es asstez "influente" 
sur les coefficients de r�gression mais nous �liminons celles dont la distance est sup�rieur ou �gal 0.03*/

/* V�rification � partir des corr�logramme*/
PROC REG DATA=tp.rlm corr;
model mpg= age poids Europe USA/dw spec vif collinoint r influence;
RUN;
QUIT;



/*Pr�diction*/

DATA tp.mpg_a_predire; SET tp.mpg_a_predire;
age=83-annee_du_modele;
IF origine='USA' THEN USA=1;
ELSE USA=0;
IF origine='Europe' THEN EUROPE=1;
ELSE EUROPE=0;
IF origine='Asie' THEN Asie=1;
ELSE Asie=0;
DROP annee_du_modele mpg;
RUN;


PROC REG DATA=tp.rlm;
model mpg= age poids Europe USA;
RUN;
QUIT;

/* mpg= 46.72248 -0.67829*age-0.00572*poids -1.17943*Europe -2.52979*USA*/

DATA tp.mpg_a_predire; SET tp.mpg_a_predire;
IF Identifiant=50 THEN mpg= 46.72248 -0.67829*12 -0.00572*2123 -1.17943*0 -2.52979*1;
IF Identifiant=160 THEN mpg=46.72248 -0.67829*8 -0.00572*3897 -1.17943*0 -2.52979*1 ;
IF Identifiant=161 THEN  mpg=46.72248 -0.67829*8-0.00572*3730 -1.17943*1 -2.52979*0;
IF Identifiant=51 THEN  mpg= 46.72248 -0.67829*12-0.00572*2074 -1.17943*1 -2.52979*0 ;
IF Identifiant=293 THEN  mpg= 46.72248 -0.67829*4-0.00572*1975 -1.17943*0 -2.52979*0 ;
IF Identifiant=302 THEN  mpg= 46.72248 -0.67829*4-0.00572*2020 -1.17943*0 -2.52979*0;
RUN;
