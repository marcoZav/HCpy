
%let folderPath=/SNM/MB/MODELLI_E_JOB;

filename shfldr filesrvc folderPath = "&folderpath./shared/";
%include shfldr('db_connections.sas');
%include shfldr('db_passthrough.sas');

