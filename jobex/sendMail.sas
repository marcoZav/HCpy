
/*

parametri
toList
subject
body

parms=toList:aaa.bbb@xx.com;ccc.ddd@yy.com|from:fff.ggg@zz.com|subject:testmail|body:corpo


debug:

data _null_;
call symput ('parms','toList:aaa.bbb@xx.com;ccc.ddd@yy.com|sender:fff.ggg@zz.com|subject:testmail|body:corpo');
run;


*/


data _null_;

length parms parm parmValue $32000;
length parmName $32 ;

parms=trim(left( symget('parms') ));
put parms=;

p=1;
do while ( scan( parms,p,'|' ) ne '' );
   parm=scan( parms,p,'|' );
   parmName = scan( parm,1,':' );
   parmValue = scan( parm,2,':' );
   call symputx (parmname, parmvalue);
   put parmname= parmvalue=;
   p=p+1;
end;

run;



filename msg email 
 to=&toList.
 from=&sender.
 subject = "&subject.";

data _null_;
 file msg;
 put "&body.";
run;


data _null_;
file _webout;

length retc $2;

if (symget('syscc') ge 4) then do;
    retc='KO';
    retMsg=symget('syserrortext');
end;
else do;
    retc='OK';
    retMsg='email sent';
end;

put '{ "retc": ' retc ', "retMsg": ' retMsg  ' }';

run;