
%let folderPath=/SNM/MB/MODELLI_E_JOB;

filename shfldr filesrvc folderPath = "&folderpath./shared/";
%include shfldr('db_connections.sas');
%include shfldr('db_passthrough.sas');



proc format;
/* la %S usa i secondi interi, la %s decimali, poi ne fornisce quanti indicati da uso del formato es. 23.3 sono 3 decimali */
picture dtpic
other='%Y-%0m-%0dT%0H:%0M:%0s' (datatype=datetime)
;
run;

data _null_;
/* meno 10 giorni, al secondo */
dt10days=datetime() -10 * (24*60*60);
dt10daysFotmatted=put(dt10days,dtpic23.3);
call symput('dtQuery',"'" || trim(left( dt10daysFotmatted ))|| "'");
run;
%put &dtQuery;

%let sql_query=
select "DATA_ORA_RIF", "ID_SCADA", "ID_UNITA_MISURA", "VALORE"  FROM "sasviya_exposure_adf"."MISURATORI_SCADA"  
WHERE  (  "DATA_ORA_RIF" >= &dtQuery);



%db_passthrough(sql_query=%nrbquote(&sql_query)
                     , outtable=STAT
                     );

%let nrecords=-1;
proc sql noprint;
select count(*) into: nrecords  from  START ;
quit;


data _null_;
file _webout;
num=compress(symget('nrecords'));
put '{ "NumPods":' 77777 '}' ;
run;