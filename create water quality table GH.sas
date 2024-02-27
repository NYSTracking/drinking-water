/****************************************************
Reproduce water sample results from Colorado SAS code
programmed by Gwen LaSelva in SAS 9.4 under windows 7
March 27, 2015 modified to reproduce the waterqualityresults table
 from the Colorado 2013 SAS code
 input: staging table sample_results, produced by other SAS program
*this program is needed because the supplied program from the EPHT sharepoint site
may produce incorrect numbers of sampling locations for HAA5's and TTHM's.
*Aug 30, 2023 lightly edit to prepare for GITHUB posting
*need to use wholesaler flag to be sure diffent sites
with same name are considered separately
******************************************************/

*get the input data from here:;
libname staging 'C:\Drinking Water\staging tables';
*put output here;
libname dw 'C:\Drinking Water\output NCDMS';
*****************************************************************
*first read in raw datafrom the Bureau of Water supply protection;
*****************************************************************;
*NonDetectFlag 1=non-detect 0=has measure;


/*format the analytes and units so we can understand them*/
proc format; /*USEPA Analyte code*/
value $ code
/*codes in data dictionary, for 10 analytes*/
"1005"="Arsenic*" 
"2050"="Atrazine*" 
"2456"="HAA5 (total Haloacetic acids)*" 	
"2950"="TTHM (total trihalomethanes?)*" 
"2039"="DEHP (DI(2-ETHYLHEXYL) PHTHALATE)*" 
"1040"="Nitrate*" 
"2987"="PCE(TETRACHLOROETHYLENE)*" 
"2984"="TCE (TRICHLOROETHYLENE)*" 
"4010"="Combined Radium 226 & 228*" 
"4006"="Uranium*"
/*other codes, from ohio epa*/
"1038"="NITRATE-NITRITE combined"   
"2450"=	"MONOCHLOROACETIC ACID"                   
"2451"=	"DICHLOROACETIC ACID"  
"2452"=	"TRICHLOROACETIC ACID"                    
"2453"=	"MONOBROMOACETIC ACID"                    
"2454"=	"DIBROMOACETIC ACID"                      
"2941"=	"CHLOROFORM"                              
"2942"=	"BROMOFORM"                               
"2943"=	"BROMODICHLOROMETHANE"                    
"2944"=	"DIBROMOCHLOROMETHANE";   
quit;

*proc contents data=staging.sample_results;run;
/* input data: 
Variable Type Len Format Informat Label 
AnalyteCode 		Num 8 8.     
ConcentrationUnits 	Char 5 $5.     
DateSampled 		Num 8 DATE9.     
DetectionLimit 	Num 8 8.3     
NonDetectFlag 		Num 8 8.   
PWSIDNumber 		Char 9 $9.  
SamplePointID 		Char 11 $11. 
concentration 		Num 8 8.3     */
/*get mean and max by pwsi and year and year/quarter. For samples coded as non-detect a concentration of half the detection limit is used before summarizing.
Note: For Nitrate, Arsenic, and the six new analytes, annual [and quarterly for Nitrate and Atrazine only] 
average concentration values are derived from first averaging by sampling station, then averaging by CWS.  
For disinfection-by-products (TTHM and HAA5) annual and quarterly average concentration values are derived from 
first averaging by day, then by CWS.   Maximums for all 10 analytes are derived by taking the annual maximum for each CWS.*/
/*quarterly for Nitrate, Atrazine, TTHM, and HAA5 only=1040 2050 2950 2456


/*285098 before sort ans subset*/
proc sort data=staging.sample_results
	out=prestandard; by PWSIDNumber analytecode SamplePointID wholesaler DateSampled; run;


data standard; 
format SamplePointID $25.;
set prestandard;
if wholesaler='Y' then SamplePointID=catx('-',SourceFacSellerPWSId,SamplePointID);
else SamplePointID=catx('-',PWSIDnumber,SamplePointID);
/*the lines below may be redundantif the sample table was preparted correclty*/
if (NonDetectFlag=1 or concentration=0) and/*if sample has no detectable concentration of the analyte*/
	not missing(DetectionLimit) and detectionlimit>0 /*has detection limit*/ then concentration=DetectionLimit/2;
/*for Radium and Atrazine, half LDL is rounded, so should be 0.02 not 0.015.  Fix May 2016*/
if analytecode=4010 and concentration<0.02  then concentration=0.02;
if analytecode=2050 and concentration<0.002 then concentration=0.002;
run;

/*first, by sampling station and year (change from day)*/
proc sort data=standard; by PWSIDnumber analytecode SamplePointID; run;
proc means data=standard (where=(not missing(concentration) and analytecode not in (2950 2456 /*TTHM and HAA5*/))) noprint nway;
by PWSIDNumber analytecode SamplePointID;
class DateSampled;
output out=byss  mean(concentration)=meanconc sum(NonDetectFlag)=NonDetectFlag IDGROUP(Max(datesampled) out(DateSampled)=Maxdate);
format DateSampled year4.;
run;
/*by day only*/
proc sort data=standard; by PWSIDNumber analytecode DateSampled; run;
proc means data=standard (where=(not missing(concentration) and analytecode in (2950 2456 /*TTHM and HAA5*/))) noprint nway;
by PWSIDNumber analytecode DateSampled;
output out=byday mean(concentration)=meanconc sum(NonDetectFlag)=NonDetectFlag 
	IDGROUP(Max(datesampled) out(DateSampled)=Maxdate);
run;
data formeans (drop=_type_ DateSampled rename=(_freq_=sample_count Maxdate=DateSampled)); set byss byday;run;
/*get means*/
proc sort data=formeans; by PWSIDNumber analytecode DateSampled;run;
proc means data=formeans noprint nway;
by PWSIDNumber analytecode;
class DateSampled;
output out=byyearavg (drop=_:) mean(meanconc)=meanconc sum(sample_count)=NumSamples sum(NonDetectFlag)=NumNonDetects 
	IDGROUP(Max(datesampled) out(DateSampled)=Maxdate);
format DateSampled year4.;
run;
/*get maximums*/
proc means data=standard (where=(not missing(concentration))) noprint nway;
by PWSIDNumber  analytecode;
class DateSampled;
output out=byyearmax(drop=_:)  max(concentration)=maxconc n(concentration)=NumSamples sum(NonDetectFlag)=NumNonDetects 
	IDGROUP(Max(datesampled) out(DateSampled)=Maxdate);
format DateSampled year4.;
run;
/*get number of sampling locations*/
proc freq data=standard noprint;
by PWSIDNumber analytecode DateSampled;
table SamplePointID /out=byyearsplist (drop=percent count);
format DateSampled year4.;
run;
proc means data=byyearsplist noprint nway;
by PWSIDNumber  analytecode;
class DateSampled;
output out=byyearsp (drop=_:) n(DateSampled)=NumSamplingLocations;
format DateSampled year4.;
run;
data byyearsp (drop=datesampled); set byyearsp; year=year(DateSampled);run;
/*end getting number of sampling locations*/
/*combine means and maxs*/
Data byyearmm (drop=meanconc maxconc DateSampled rename=(MaxDate=DateSampled));
format DateSampled MaxDate date9. AggregationType $2.;
set byyearavg (in=a) byyearmax (in=m);
by PWSIDNumber analytecode DateSampled;
year=year(DateSampled);
if a=1 then do; AggregationType="X "; concentration=meanconc;  end;
if m=1 then do; AggregationType="MX"; concentration=maxconc; end;/*may only need max for annual data*/
run;
proc sort data=byyearmm; by PWSIDNumber analytecode year;run;
proc sort data=byyearsp; by PWSIDNumber analytecode year; run;
/*merge in number of sampling locations*/
data byyear;
merge byyearmm byyearsp;
by PWSIDNumber analytecode year;
run;
*proc print data=byyear(obs=10);run;
/**********************************************
by quarter
**********************************************************/
/*get means*/
proc sort data=standard; by PWSIDNumber analytecode SamplePointID; run;
proc means data=standard (where=(AnalyteCode in (1040 2050 /*Nirate and Atrazine*/))) noprint nway;
by PWSIDNumber analytecode SamplePointID;
class DateSampled;
output out=byqss mean(concentration)=meanconc sum(NonDetectFlag)=NonDetectFlag
	IDGROUP(Max(datesampled) out(DateSampled)=Maxdate);
format DateSampled yyqd6.;
run; /*38532 obs*/
data formeans2 (drop=_type_ DateSampled rename=(_freq_=sample_count Maxdate=DateSampled)); set byqss byday;run;

proc sort data=formeans2; by PWSIDNumber analytecode DateSampled;run;
proc means data=formeans2 (where=(AnalyteCode in (2950 2456 1040 2050))) noprint nway; /*all four*/
by PWSIDNumber analytecode;
class DateSampled;
output out=byqavg (drop=_:) mean(meanconc)=meanconc sum(sample_count)=NumSamples sum(NonDetectFlag)=NumNonDetects
	IDGROUP(Max(datesampled) out(DateSampled)=Maxdate);
format DateSampled yyqd6.;
run; /*76529 obs*/

/*get number of sampling locations*/
proc sort data=standard; by PWSIDNumber analytecode DateSampled;run;
proc freq data=standard (where=(AnalyteCode in (1040 2050 2950 2456))) noprint;
by PWSIDNumber analytecode DateSampled;
table SamplePointID /out=byqsplist (drop=percent count);
format DateSampled yyqd6.;
run; /*datesampled is a full date value, one obs for each sample point*/
proc means data=byqsplist noprint nway;
by PWSIDNumber  analytecode;
class DateSampled;
output out=byqsp (drop=_:) n(DateSampled)=NumSamplingLocations;
format DateSampled yyqd6.;
run;
data byqsp (drop=DateSampled); set byqsp; year=year(DateSampled); quarter=put(DateSampled, yyqd6.);run;
/*end getting number of sampling locations*/
Data byqavg; set byqavg; quarter=put(DateSampled, yyqd6.);run;

proc sort data=byqavg; by PWSIDNumber analytecode quarter;run;

Data byquartermm (drop=/*_: maxconc*/ meanconc DateSampled rename=(Maxdate=DateSampled));
format DateSampled MaxDate date9. AggregationType $2.;
set byqavg (in=a) /*byqmax (in=m)*/;
year=year(DateSampled); quarter=put(DateSampled, yyqd6.);
if a=1 then do; AggregationType="X "; concentration=meanconc;  end;
run;

proc sort data=byquartermm; by PWSIDNumber analytecode quarter;run;
proc sort data=byqsp; by PWSIDNumber analytecode quarter; run;
/*merge in number of sampling locations*/
data byquarter;
merge byquartermm byqsp;
by PWSIDNumber analytecode quarter;
run;

data waterquality (drop=quarter dateSampled concentration  rename=(date=DateSampled charconcentration=concentration));
format SummaryTimePeriod $6. date $10. ConcentrationUnits $5. AnalyteCode 4.;
set byyear(in=a) byquarter(in=b);
if missing(concentration) then delete; /*if missing, do not report means etc*/
if a=1 then SummaryTimePeriod=put(year,4.);
if b=1 then SummaryTimePeriod=quarter;
date=put(DateSampled,yymmdd10.);
concentration=round(Concentration,0.0001);
/*convert the concentration to a character variable, since the xml output will be character
should follow the perl regular expression pattern '[0-9]{1,6}\.[0-9]{4}|-[8]{3}' */
if Concentration NE -888 then Charconcentration=put(Concentration,11.4);
else charconcentration="-888";
/*add concentration units*/
if AnalyteCode not in (1040 4010) then ConcentrationUnits="ug/L";
else if AnalyteCode in (1040)  then ConcentrationUnits="mg/L";
else if AnalyteCode=4010 then ConcentrationUnits="pCi/L"; 
run; 
proc sort data=waterquality; by year PWSIDNumber AnalyteCode DateSampled;run;
data dw.waterqualityny (drop=n compress=char label="This is 2022 EPHT water quality data for submission in 2023 during 2nd window");
*variable names are case sensitive;
retain RowIdentifier PWSIDNumber Year AnalyteCode DateSampled AggregationType NumSamplingLocations SummaryTimePeriod 
	NumSamples NumNonDetects ConcentrationUnits Concentration;
set waterquality; by year;
array myvar(*) _char_;
if missing(AnalyteCode) then delete; /*a few obs have water supply data but no sample -so delete these, investiagate later*/
/*some special characters need special handling*/
do n=1 to dim(myvar);
   myvar(n)=tranwrd(myvar(n),'&','&amp;');
   myvar(n)=tranwrd(myvar(n),'<','&lt;');
   myvar(n)=tranwrd(myvar(n),'>','&gt;');
   myvar(n)=tranwrd(myvar(n),"'",'&apos;'); 
   myvar(n)=tranwrd(myvar(n),'"','&quot;');
end;
if first.year then RowIdentifier=0;
RowIdentifier=RowIdentifier+1;
run; 
/*check for duplicates.  If all is good, check will have no observations */
proc sort data=dw.waterqualityny (drop=rowidentifier) out=check nouniquekey; by _all_;run;

/*output CSV files for XML generator, need to edit years according to the data*/
ods results off;
ods csv file='P:\Sections\EHS\EPHT\Indicators\Indicators_2023\Drinking Water\output NCDMS\WQL2020.csv';
proc print data=dw.waterqualityny (where=(year in (2020))) noobs; run;
ods csv close;
ods csv file='P:\Sections\EHS\EPHT\Indicators\Indicators_2023\Drinking Water\output NCDMS\WQL2021.csv';
proc print data=dw.waterqualityny (where=(year in (2021 ))) noobs; run;
ods csv close;
ods csv file='P:\Sections\EHS\EPHT\Indicators\Indicators_2023\Drinking Water\output NCDMS\WQL2022.csv';
proc print data=dw.waterqualityny (where=(year in (2022))) noobs; run;
ods csv close;
ods results on;
