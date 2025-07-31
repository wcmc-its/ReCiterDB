
DELIMITER ////
CREATE DEFINER=`admin`@`%` PROCEDURE `generateBibliometricReport`(IN personID VARCHAR(255))
BEGIN

set @personIdentifier = personID;

set @countRecent = 0;
set @countRecent = (select 
count(a.pmid)
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' and 
a.personIdentifier = @personIdentifier and 
percentileNIH is null and  
round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24),0) < 730
limit 10
);



set @countOlder = 0;
set @countOlder = (select 
count(a.pmid)
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where 
publicationTypeNIH = 'Research Article' and 
a.personIdentifier = @personIdentifier and 
percentileNIH is null and  
articleYear < 1980
);




set @currentdate = (select DATE_FORMAT(curdate(),'%M %e, %Y'));

select countAll, countFirst, countSenior, department, lower(facultyRank), nameFirst, nameLast, nameMiddle, top10DenominatorAll, top10DenominatorFirst, top10DenominatorFirstSenior, top10DenominatorSenior, concat(round(top10PercentileAll,1)), concat(round(top10PercentileFirst,1)), concat(round(top10PercentileFirstSenior,1)), concat(round(top10PercentileSenior,1)), top10RankAll, top10RankFirst, top10RankFirstSenior, top10RankSenior, top5DenominatorAll, top5DenominatorFirst, top5DenominatorFirstSenior, top5DenominatorSenior, concat(round(top5PercentileAll,1)), concat(round(top5PercentileFirst,1)), concat(round(top5PercentileFirstSenior,1)), concat(round(top5PercentileSenior,1)), top5RankAll, top5RankFirst, top5RankFirstSenior, top5RankSenior from analysis_summary_person 
where personIdentifier = @personIdentifier 
into @countAll, @countFirst, @countSenior, @department, @facultyRank, @nameFirst, @nameLast, @nameMiddle, @top10DenominatorAll, @top10DenominatorFirst, @top10DenominatorFirstSenior, @top10DenominatorSenior, @top10PercentileAll, @top10PercentileFirst, @top10PercentileFirstSenior, @top10PercentileSenior, @top10RankAll, @top10RankFirst, @top10RankFirstSenior, @top10RankSenior, @top5DenominatorAll, @top5DenominatorFirst, @top5DenominatorFirstSenior, @top5DenominatorSenior, @top5PercentileAll, @top5PercentileFirst, @top5PercentileFirstSenior, @top5PercentileSenior, @top5RankAll, @top5RankFirst, @top5RankFirstSenior, @top5RankSenior;


## Newer papers

select concat('https://pubmed.gov/?term=',
group_concat(x.pmid order by x.pmid desc separator '+'))
from (
select a.personIdentifier, a.pmid
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and percentileNIH is null 
and a.personIdentifier = @personIdentifier
and round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24),0) < 730
order by a.pmid desc
limit 220 offset 0) x
into @newerPubsPubMedURL;



## Older papers

select concat('https://pubmed.gov/?term=',
group_concat(x.pmid order by x.pmid desc separator '+'))
from (
select a.personIdentifier, a.pmid
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and articleYear < 1980
and percentileNIH is null 
and a.personIdentifier = @personIdentifier
order by authorPosition desc, a.pmid desc
limit 220 offset 0) x
into @olderPubsPubMedURL;




## All research articles - any author position and any date

## Word forbids us from including links with >2,270 characters, so we can only use a max of 220 PMIDs for each URL.
## Here, we're heroically trying to output these in descending order of significance.


truncate analysis_temp_article;

insert into analysis_temp_article (pmid, position)
select distinct
pmid, authorPosition as position
from (
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 1 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and percentileNIH is not null 
and authorPosition is not null
and a.personIdentifier = @personIdentifier
) 
UNION
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 2 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and percentileNIH is not null 
and authorPosition is null
and a.personIdentifier = @personIdentifier) 
UNION
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 3 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and articleYear < 1980
and authorPosition is not null
and a.personIdentifier = @personIdentifier)
UNION
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 4 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and authorPosition is not null
and a.personIdentifier = @personIdentifier
and round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24),0) < 730)
UNION
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 5 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and authorPosition is null
and a.personIdentifier = @personIdentifier
and round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24),0) < 730)
UNION
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 6 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and articleYear < 1980
and authorPosition is null
and a.personIdentifier = @personIdentifier)
UNION
(select a.personIdentifier, a.pmid, authorPosition, percentileNIH, relativeCitationRatioNIH, citationCountNIH, readersMendeley, 6 as ranking
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and a.personIdentifier = @personIdentifier)
) x 
order by ranking asc, percentileNIH desc, authorPosition desc, relativeCitationRatioNIH desc, citationCountNIH desc, readersMendeley desc;




## Get counts of pubs for "Additional Information" section

set @firstAuthoredPubsCount = 0;
set @allAuthoredPubsCount = 0;
set @lastAuthoredPubsCount = 0;


select count(distinct a.pmid)
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and a.personIdentifier = @personIdentifier
into @allAuthoredPubsCount;

select count(distinct a.pmid)
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and authorPosition = 'last'
and a.personIdentifier = @personIdentifier
into @seniorAuthoredPubsCount;

select count(distinct a.pmid)
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and authorPosition = 'first'
and a.personIdentifier = @personIdentifier
into @firstAuthoredPubsCount;



## Create PubMed URLs for any author position for "Additional Information" section

## Last-authored articles

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'last' 
order by id asc
limit 220 offset 0) x 
into @seniorAuthoredPubsURL1; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'last' 
order by id asc
limit 220 offset 220) x 
into @seniorAuthoredPubsURL2; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'last' 
order by id asc
limit 220 offset 440) x 
into @seniorAuthoredPubsURL3; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'last' 
order by id asc
limit 220 offset 660) x 
into @seniorAuthoredPubsURL4; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'last' 
order by id asc
limit 220 offset 880) x 
into @seniorAuthoredPubsURL5; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'last' 
order by id asc
limit 220 offset 1100) x 
into @seniorAuthoredPubsURL6; 


## First-authored articles

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'first' 
order by id asc
limit 220 offset 0) x 
into @firstAuthoredPubsURL1; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'first' 
order by id asc
limit 220 offset 220) x 
into @firstAuthoredPubsURL2; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'first' 
order by id asc
limit 220 offset 440) x 
into @firstAuthoredPubsURL3; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'first' 
order by id asc
limit 220 offset 660) x 
into @firstAuthoredPubsURL4; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'first' 
order by id asc
limit 220 offset 880) x 
into @firstAuthoredPubsURL5; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
where position = 'first' 
order by id asc
limit 220 offset 1100) x 
into @firstAuthoredPubsURL6; 



## Any author position

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
order by id asc
limit 220 offset 0) x 
into @allAuthoredPubsURL1; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
order by id asc
limit 220 offset 220) x 
into @allAuthoredPubsURL2; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
order by id asc
limit 220 offset 440) x 
into @allAuthoredPubsURL3; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
order by id asc
limit 220 offset 660) x 
into @allAuthoredPubsURL4; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
order by id asc
limit 220 offset 880) x 
into @allAuthoredPubsURL5; 

select concat('https://pubmed.gov/?term=', group_concat(distinct pmid order by id asc separator '+')) 
from 
(select distinct id, pmid from analysis_temp_article 
order by id asc
limit 220 offset 1100) x 
into @allAuthoredPubsURL6; 




## Get h-index values

select hindexNIH from analysis_summary_person where personIdentifier = @personIdentifier
into @hindex;

select h5indexNIH from analysis_summary_person where personIdentifier = @personIdentifier
into @h5index;



## Individual records for influential first or last authored papers

set @11_influence_first_last = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by percentileNIH desc, relativeCitationRatioNIH desc),
".",
case 
  when authors like "((%" then replace(replace(authorsRTF,"((","\\'a0
\\f0\\b "),"))","
\\f1\\b0")
  when authors like "%((%" then concat("\\'a0",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0"))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"",
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f4 .\\

",
"\\f1 Citation Count: ",
citationCountNIH,
"\\'a0 |\\'a0 NIH percentile: ",
concat(round(percentileNIH,1)),
"\\'a0 |\\'a0 Relative Citation Ratio: ",
concat(round(relativeCitationRatioNIH,2)),
"\\
\\

") as y
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where 
publicationTypeNIH = 'Research Article' and 
authorPosition in ('first','last') and 
a.personIdentifier = @personIdentifier 
order by percentileNIH desc, relativeCitationRatioNIH desc
limit 10) x);
  
   
    
     



## Individual records for influential papers, any author position

set @13_influence_all = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by percentileNIH desc, relativeCitationRatioNIH desc),
". ",
case 
  when authors like "((%" then replace(replace(authorsRTF,"((","
\\f0\\b "),"))","
\\f1\\b0")
  when authors like "%((%" then concat("",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0"))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"",
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f4 .\\

",
"\\f1 Citation Count: ",
citationCountNIH,
"\\'a0 |\\'a0 NIH percentile: ",
concat(round(percentileNIH,1)),
"\\'a0 |\\'a0 Relative Citation Ratio: ",
concat(round(relativeCitationRatioNIH,1)),
"\\
\\
") as y
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where 
publicationTypeNIH = 'Research Article' and 
a.personIdentifier = @personIdentifier 
order by percentileNIH desc, relativeCitationRatioNIH desc
limit 10) x);



## Individual records for recent articles


set @15_recent_articles = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by authorPosition desc, readersMendeley desc),
". ",
case 
  when authors like "((%" then replace(replace(authorsRTF,"((","
\\f0\\b "),"))","
\\f1\\b0")
  when authors like "%((%" then concat("",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0"))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f4 .\\

",
"\\f1 Mendeley Readers: ",
case 
when readersMendeley is not null then readersMendeley
else "N/A"
end,
"\\'a0 |\\'a0 Citation Count: ",
case 
when citationCountNIH is not null then citationCountNIH
else "N/A"
end,
"\\'a0 |\\'a0 Journal Impact Score: ",
case 
when JournalImpactScore1 is not null then JournalImpactScore1
else "N/A"
end,
"\\
\\
") as y
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where 
publicationTypeNIH = 'Research Article' and 
a.personIdentifier = @personIdentifier and
percentileNIH is null and  
round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24),0) < 730
order by authorPosition desc, readersMendeley desc
limit 10) x);



## Individual records for pre-1980 articles


set @16a_older_articles = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by authorPosition desc, citationCountNIH desc),
". ",
case 
  when authors like "((%" then replace(replace(authorsRTF,"((","
\\f0\\b "),"))","
\\f1\\b0")
  when authors like "%((%" then concat("",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0"))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f4 .\\

",
"\\f1 Citation Count: ",
case 
when citationCountNIH is not null then citationCountNIH
else "N/A"
end,
"\\'a0 |\\'a0 Journal Impact Score: ",
case 
when JournalImpactScore1 is not null then JournalImpactScore1
else "N/A"
end,
"\\
\\
"
) as y
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where 
articleYear < 1980 and 
publicationTypeNIH = 'Research Article' and 
a.personIdentifier = @personIdentifier and
percentileNIH is null   
order by authorPosition desc, citationCountNIH desc
limit 10) x);

#########


set @1_before_title = "{\\rtf1\\ansi\\ansicpg1252\\cocoartf2580
\\cocoatextscaling0\\cocoaplatform0{\\fonttbl\\f0\\fswiss\\fcharset0 Arial-BoldMT;\\f1\\fswiss\\fcharset0 ArialMT;\\f2\\fswiss\\fcharset0 Helvetica;
\\f3\\fswiss\\fcharset0 Arial-ItalicMT;\\f4\\froman\\fcharset0 TimesNewRomanPSMT;\\f5\\fnil\\fcharset128 MS-Gothic;
}
{\\colortbl;\\red255\\green255\\blue255;\\red38\\green38\\blue38;\\red42\\green93\\blue160;\\red5\\green99\\blue193;
}
{\\*\\expandedcolortbl;;\\csgenericrgb\\c14902\\c14902\\c14902;\\csgenericrgb\\c16471\\c36471\\c62745;\\csgenericrgb\\c1961\\c38824\\c75686;
}
{\\*\\listtable{\\list\\listtemplateid1\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid1\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listname ;}\\listid1}
{\\list\\listtemplateid2\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid101\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid102\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid2}
{\\list\\listtemplateid3\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid201\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid202\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid3}
{\\list\\listtemplateid4\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid301\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid302\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid4}}
{\\*\\listoverridetable{\\listoverride\\listid1\\listoverridecount0\\ls1}{\\listoverride\\listid2\\listoverridecount0\\ls2}{\\listoverride\\listid3\\listoverridecount0\\ls3}{\\listoverride\\listid4\\listoverridecount0\\ls4}}
\\margl1440\\margr1440\\vieww23220\\viewh13800\\viewkind1\\viewscale150
\\deftab720
\\pard\\tx720\\tx1440\\tx2160\\tx2880\\tx3600\\tx4320\\tx5040\\tx5760\\tx6480\\tx7200\\tx7920\\tx8640\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0";

set @1a_before_title = "{\\rtf1\\ansi\\ansicpg1252\\cocoartf2580
\\cocoatextscaling0\\cocoaplatform0{\\fonttbl\\f0\\fswiss\\fcharset0 Arial-BoldMT;\\f1\\fswiss\\fcharset0 ArialMT;\\f2\\fswiss\\fcharset0 Arial-ItalicMT;
\\f3\\froman\\fcharset0 TimesNewRomanPSMT;}
{\\colortbl;\\red255\\green255\\blue255;\\red38\\green38\\blue38;\\red42\\green93\\blue160;\\red5\\green99\\blue193;
}
{\\*\\expandedcolortbl;;\\csgenericrgb\\c14902\\c14902\\c14902;\\csgenericrgb\\c16471\\c36471\\c62745;\\csgenericrgb\\c1961\\c38824\\c75686;
}
\\margl1440\\margr1440\\vieww22580\\viewh14380\\viewkind1\\viewscale150
\\deftab720
\\pard\\tx720\\tx1440\\tx2160\\tx2880\\tx3600\\tx4320\\tx5040\\tx5760\\tx6480\\tx7200\\tx7920\\tx8640\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f0\\b\\fs36 \\cf2 ";

#########

set @2a_h1_header = concat("Analysis of ",@nameFirst," ",@nameLast, "'s research articles
\\f1\\b0\\fs24 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0");


set @2_h1_header = concat("\\f0\\b\\fs36 \\cf2 Analysis of ",@nameFirst," ",@nameLast, "'s research articles
\\f1\\b0\\fs24 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0");


#########

set @3_intro_paragraph = 
concat("\\fs22 \\cf2 This is a bibliometric analysis of research articles from PubMed authored by ",@nameFirst," ",@nameLast," (person identifier: ",@personIdentifier,") in ",@department,". It was prepared by the Weill Cornell Medical Library on ",@currentdate,". The terms used in this report are described in the \"Explanation\" section at the end.\\
\\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

");

#########



set @4_summary_bullet1 = concat("
\\fs32 \\cf2 Summary\\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs22 \\cf2 For the purpose of this analysis, ",@nameLast," has authored\\'a0",
"\\f0\\b ",
@countAll,
"\\f1\\b0 ",
"\\'a0research articles appearing in PubMed,\\'a0",
"\\f0\\b ",
@countFirst,
"\\f1\\b0 \\'a0of which were written as a first author and\\'a0
\\f0\\b ",
@countSenior," ",
"\\f1\\b0as a senior author. Any articles written in the last two years or prior to 1980 are excluded from this analysis, but are included in either the \"Select recent articles\" or \"Select older articles\" sections.\\
\\pard\\tx720\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\cf2 \\
Articles' NIH percentile by author position:\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb240\\sa120\\partightenfactor0
");

#########


set @5_summary_bullet2 = concat("\\f0\\b \\cf2 First or senior 
\\f1\\b0 - The five and ten most influential first- or senior-authored research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileFirstSenior," 
\\f1\\b0 and\\'a0
\\f0\\b ",@top10PercentileFirstSenior,", ","
\\f1\\b0 respectively (higher is better).\\'a0There are ",@top5DenominatorFirstSenior," WCM-employed full-time faculty at the ",@facultyRank," level with five or more first- or senior-authored research articles, and ",@top10DenominatorFirstSenior," who have written ten or more. Within these groups, ",@nameLast,"'s percentiles rank at 
\\f0\\b #",@top5RankFirstSenior," ","
\\f1\\b0 and 
\\f0\\b #",@top10RankFirstSenior,"
\\f1\\b0, respectively (lower is better).\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");


set @5_summary_bullet2a = concat("\\f0\\b \\cf2 First or senior 
\\f1\\b0 - The five most influential first- or senior-authored research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileFirstSenior,"
\\f1\\b0, (higher is better).\\'a0There are ",@top5DenominatorFirstSenior," WCM-employed full-time faculty at the ",@facultyRank," level with five or more first- or senior-authored research articles. Within this group, this percentile ranks at 
\\f0\\b #",@top5RankFirstSenior,"
\\f1\\b0\\'a0(lower is better).\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");

set @5_summary_bullet2b = concat("\\f0\\b \\cf2 first- or senior 
\\f1\\b0 - ",@nameLast," has fewer than five first- or senior-authored research articles, so the average NIH percentile is not provided.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");



#########


set @6_summary_bullet3 = concat("\\f0\\b \\cf2 Senior 
\\f1\\b0 - The five and ten most influential senior-authored research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileSenior," 
\\f1\\b0 and\\'a0
\\f0\\b ",@top10PercentileSenior,"
\\f1\\b0, respectively. There are ",@top5DenominatorSenior," WCM-employed full-time faculty at the ",@facultyRank," level with five or more senior-authored research articles, and ",@top10DenominatorSenior," who have written ten or more. Within these groups, ",@nameLast,"'s percentiles rank at 
\\f0\\b #",@top5RankSenior," ","
\\f1\\b0 and 
\\f0\\b #",@top10RankSenior,"
\\f1\\b0, respectively.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");

set @6_summary_bullet3a = concat("\\f0\\b \\cf2 Senior 
\\f1\\b0 - The five most influential senior-authored research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileSenior,".
\\f1\\b0\\'a0There are ",@top5DenominatorSenior," WCM-employed full-time faculty at the ",@facultyRank," level with five or more senior-authored research articles. Within this group, this percentile ranks at 
\\f0\\b #",@top5RankSenior,"
\\f1\\b0.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");

set @6_summary_bullet3b = concat("\\f0\\b \\cf2 Senior 
\\f1\\b0 - ",@nameLast," has fewer than five senior-authored research articles, so the average NIH percentile is not provided.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");


#########

set @7_summary_bullet4 = concat("\\f0\\b \\cf2 First 
\\f1\\b0 - The five and ten most influential first-authored research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileFirst," 
\\f1\\b0 and\\'a0
\\f0\\b ",@top10PercentileFirst,"
\\f1\\b0, respectively. There are ",@top5DenominatorFirst," WCM-employed full-time faculty at the ",@facultyRank," level with five or more first-authored research articles, and ",@top10DenominatorFirst," who have written ten or more. Within these groups, ",@nameLast,"'s percentiles rank at 
\\f0\\b #",@top5RankFirst," ","
\\f1\\b0 and 
\\f0\\b #",@top10RankFirst,"
\\f1\\b0, respectively.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");


set @7_summary_bullet4a = concat("\\f0\\b \\cf2 First 
\\f1\\b0 - The five most influential first-authored research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileFirst,".
\\f1\\b0\\'a0There are ",@top5DenominatorFirst," WCM-employed full-time faculty at the ",@facultyRank," level with five or more first-authored research articles. Within this group, this percentile ranks at 
\\f0\\b #",@top5RankFirst,"
\\f1\\b0.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");

set @7_summary_bullet4b = concat("\\f0\\b \\cf2 First 
\\f1\\b0 - ",@nameLast," has fewer than five first-authored research articles, so the average NIH percentile is not provided.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");


#########

set @8_summary_bullet5 = concat("\\f0\\b \\cf2 Any position 
\\f1\\b0 - The five and ten most influential research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileAll," 
\\f1\\b0 and\\'a0
\\f0\\b ",@top10PercentileAll,"
\\f1\\b0, respectively. There are ",@top5DenominatorAll," WCM-employed full-time faculty at the ",@facultyRank," level with five or more authored research articles, and ",@top10DenominatorAll," who have written ten or more. Within these groups, ",@nameLast,"'s percentiles rank at 
\\f0\\b #",@top5RankAll," ","
\\f1\\b0 and 
\\f0\\b #",@top10RankAll,"
\\f1\\b0, respectively.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");


set @8_summary_bullet5a = concat("\\f0\\b \\cf2 Any position 
\\f1\\b0 - The five most influential research articles have an average NIH percentile of\\'a0
\\f0\\b ",@top5PercentileAll,"."," 
\\f1\\b0\\'a0There are ",@top5DenominatorAll," WCM-employed full-time faculty at the ",@facultyRank," level with five or more authored research articles. Within this group, this percentile ranks at 
\\f0\\b #",@top5RankAll,"
\\f1\\b0.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");


set @8_summary_bullet5b = concat("\\f0\\b \\cf2 Any position 
\\f1\\b0 - Individual has fewer than five research articles, so the average NIH percentile is not provided.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");



#########

set @10_influence_intro = concat("
\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs32 \\cf2 Influence\\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs22 \\cf2 The following are the most influential research articles authored by ",@nameFirst," ",@nameLast," according to\\'a0{\\field{\\*\\fldinst{HYPERLINK \"https://icite.od.nih.gov/\"}}{\\fldrslt \\cf3 iCite}},\\'a0a\\'a0tool developed and endorsed by the NIH to judge article influence citation counts.\\
");

#########

set @10a_influence_first_last_intro = 
case when @countFirst + @countSenior > 0 then "\\f0\\b\\fs28 \\cf2 \\
First or senior author position only\\
\\f1\\b0\\fs24 \\cf2 \\"
else "" 
end;


set @11b_influence_first_last = concat(" 
\\f1\\b0 - Individual has no first- or senior-authored research articles.\\
\\pard\\tx720\\pardeftab720\\li730\\fi7\\ri0\\sl264\\slmult1\\sb120\\sa120\\partightenfactor0");



set @12_influence_any_position = "\\f0\\b\\fs28 \\cf2 \\
Any position\\
\\f1\\b0\\fs24 \\cf2 \\";


#########


set @14_recent_intro = 
case when @countRecent > 0 then concat("\\f0\\b\\fs28 \\cf2 \\
Select recent articles\\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f1\\b0\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs22 \\cf2 The following do not yet have an NIH percentile because they were published in the past two years (see {\\field{\\*\\fldinst{HYPERLINK \"",@newerPubsPubMedURL,"\"}}{\\fldrslt \\cf3 list}}).\\
\\f4\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0")
else "\\f0\\b\\fs28 \\cf2 \\
Select recent articles\\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f1\\b0\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs22 \\cf2 No research articles have been published in the past two years.
\\f4\\fs32 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f1\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0" 
end;



set @16_older_intro = 
case when @countOlder > 0 then concat("\\f0\\b\\fs28 \\cf2 \\
Select older articles\\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f1\\b0\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs22 \\cf2 Neither a Relative Citation Ratio nor an NIH percentile is available for the following because they were published prior to 1980 (see {\\field{\\*\\fldinst{HYPERLINK \"",@olderPubsPubMedURL,"\"}}{\\fldrslt \\cf3 list}}).\\

\\f1\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0")
else null 
end;



set @19_additional_stats = concat("
\\fs24 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\fs36 Additional statistics
\\f0\\b\\fs28 \\cf2 \\
\\pard\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\f1\\b0\\fs22 \\cf2 \\
\\pard\\tx720\\tx1440\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\cf2 
",@nameLast," has an h-index of\\'a0
\\f0\\b \\cf2 ",@hindex," ","
\\f1\\b0 \\cf2 and an h5-index of\\'a0
\\f0\\b \\cf2 ",@h5index,"
\\f1\\b0 \\cf2 \\'a0when using citation counts from iCite.\\
\\
The following are complete lists of PubMed records for articles authored by ",@nameLast,":\\
\\
\\pard\\pardeftab720\\li397\\fi27\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0


\\f0\\b\\fs22 \\cf2 ",@allAuthoredPubsCount, 

case
when @allAuthoredPubsCount = 0 then "\\f1\\b0 \\cf2 research articles\\"
when @allAuthoredPubsCount <= 220 then concat("\\f1\\b0 \\cf2  research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL1 ,"\"}}{\\fldrslt list}}\\")
when @allAuthoredPubsCount <= 440 then concat("\\f1\\b0 \\cf2  research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}} and {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}\\")
when @allAuthoredPubsCount <= 660 then concat("\\f1\\b0 \\cf2  research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}\\")
when @allAuthoredPubsCount <= 880 then concat("\\f1\\b0 \\cf2  research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}\\")
when @allAuthoredPubsCount <= 1100 then concat("\\f1\\b0 \\cf2  research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL5 ,"\"}}{\\fldrslt #5}},\\")
else concat("\\f1\\b0 \\cf2  all-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}, {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL5 ,"\"}}{\\fldrslt #5}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @allAuthoredPubsURL6 ,"\"}}{\\fldrslt #6}}\\")
end, 
"

\\fs22 \\
\\f0\\b\\fs22 \\cf2 ",@seniorAuthoredPubsCount, 

case
when @seniorAuthoredPubsCount = 0 then "\\f1\\b0 \\cf2  senior-authored research articles\\"
when @seniorAuthoredPubsCount <= 220 then concat("\\f1\\b0 \\cf2  senior-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL1 ,"\"}}{\\fldrslt list}}\\")
when @seniorAuthoredPubsCount <= 440 then concat("\\f1\\b0 \\cf2  senior-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}} and {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}\\")
when @seniorAuthoredPubsCount <= 660 then concat("\\f1\\b0 \\cf2  senior-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}\\")
when @seniorAuthoredPubsCount <= 880 then concat("\\f1\\b0 \\cf2  senior-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}\\")
when @seniorAuthoredPubsCount <= 1100 then concat("\\f1\\b0 \\cf2  senior-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL5 ,"\"}}{\\fldrslt #5}},\\")
else concat("\\f1\\b0 \\cf2  senior-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}, {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL5 ,"\"}}{\\fldrslt #5}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @seniorAuthoredPubsURL6 ,"\"}}{\\fldrslt #6}}\\")
end, 
"

\\fs22 \\
\\f0\\b\\fs22 \\cf2 ",@firstAuthoredPubsCount, 
case
when @firstAuthoredPubsCount = 0 then "\\f1\\b0 \\cf2  first-authored research articles\\"
when @firstAuthoredPubsCount <= 220 then concat("\\f1\\b0 \\cf2  first-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL1 ,"\"}}{\\fldrslt list}}\\")
when @firstAuthoredPubsCount <= 440 then concat("\\f1\\b0 \\cf2  first-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}} and {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}\\")
when @firstAuthoredPubsCount <= 660 then concat("\\f1\\b0 \\cf2  first-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}\\")
when @firstAuthoredPubsCount <= 880 then concat("\\f1\\b0 \\cf2  first-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}\\")
when @firstAuthoredPubsCount <= 1100 then concat("\\f1\\b0 \\cf2  first-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL5 ,"\"}}{\\fldrslt #5}},\\")
else concat("\\f1\\b0 \\cf2  first-authored research articles; see {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL1 ,"\"}}{\\fldrslt list #1}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL2 ,"\"}}{\\fldrslt #2}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL3 ,"\"}}{\\fldrslt #3}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL4 ,"\"}}{\\fldrslt #4}}, {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL5 ,"\"}}{\\fldrslt #5}}, and {\\field{\\*\\fldinst{HYPERLINK \"", @firstAuthoredPubsURL6 ,"\"}}{\\fldrslt #6}}\\")
end, 
"
\\pard\\tx720\\tx1440\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0
\\cf2 \\
Articles are from all years and displayed in descending order of inferred significance.\\
\\
\\
\\pard\\tx720\\tx1440\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0");



#########


set @20_ending = "\\
\\fs36 \\cf2 Explanation\\
\\pard\\tx720\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f4\\fs24 \\cf2 \\
\\pard\\tx0\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\f1\\fs22 \\cf2 iCite was a service created and endorsed by the NIH that uses citation counts to approximate the influence of research articles in PubMed.\\'a0For details, see\\'a0{\\field{\\*\\fldinst{HYPERLINK \"https://dx.doi.org/10.1371/journal.pbio.1002541\"}}{\\fldrslt \\cf3 this paper}}
\\f4 \\'a0
\\f1 or the\\'a0{\\field{\\*\\fldinst{HYPERLINK \"https://icite.od.nih.gov/user_guide?page_id=ug_overview\"}}{\\fldrslt \\cf3 User Guide}}
\\f4 .\\'a0\\
\\
\\pard\\tx720\\pardeftab720\\li399\\fi-12\\sl264\\slmult1\\sa240\\partightenfactor0

\\f0\\b \\cf2 Citation Count 
\\f1\\b0 \\cf2 is the number of citations an article has received from CrossRef, MEDLINE, PubMed Central, and Entrez. \\

\\f0\\b \\cf2 Relative Citation Ratio (RCR) 
\\f1\\b0 \\cf2 is the ratio between the number of times an article was cited in comparison to publications of the same date and field (as inferred by co-citation networks). A value of 1.0 is the median. The benchmark consists of research articles that are the product of\\'a0{\\field{\\*\\fldinst{HYPERLINK \"https://grants.nih.gov/grants/funding/r01.htm\"}}{\\fldrslt \\cf4 \\ul \\ulc4 R01 grants}}, the NIH's most prestigious and competitive funding mechanism. \\

\\f0\\b \\cf2 NIH percentile 
\\f1\\b0 \\cf2 is the value of RCR provided as a percentile in which 100 is the highest and 0 is the lowest. For example, if an article has an NIH percentile of 63.2, it has received more citations than 631 articles when measured against a field and time-weighted benchmark of 1,000 NIH-funded research articles from the same year. A percentile is not computed for an article published in the past two years.\\
\\pard\\tx0\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0

\\fs6 \\cf2 \\

\\f0\\b\\fs22 \\cf2 The list of publications is from ReCiter,
\\f1\\b0 \\cf2  Weill Cornell Medicine's authoritative publications reporting system. ReCiter is curated by Weill Cornell librarians, updated on at least a weekly basis, and validated during faculty annual review cycles and other opportunities for feedback. It contains 200,000 known publications.\\
\\

\\f0\\b \\cf2 Author position
\\f1\\b0 \\cf2  is inferred based on a name-matching heuristic, which fails to identify an author's position 1.5% of the time, and well under 1% when only considering cases where a person is actually listed among the authors in the bibliographic source as opposed to being represented as a group authorship or missing entirely. There are also cases where an author shares a senior author role but is not the last author, or shares a first author role but is not the first author. Unfortunately, such information is not available for consumption in a programmatic way from any authoritative bibliographic source. However, Weill Cornell's source system can be manually updated to correct author positions, after which this report can be re-run. Please submit such requests to {\\field{\\*\\fldinst{HYPERLINK \"mailto:publications@med.cornell.edu\"}}{\\fldrslt publications@med.cornell.edu}} and provide CWID, PMID(s), and whether the author should be first or last position. For questions regarding the role of an author's contribution, consult the full text of an article.\\
\\

\\f0\\b \\cf2 h-index
\\f1\\b0 \\cf2  is the number of an author's articles in PubMed that have been cited, as defined by iCite, at least that many times. Scopus and Web of Science usually have slightly higher times-cited values than the sources used by iCite, but the difference is typically within several percentage points. The h5-index is the number of an author's articles from the past five years in PubMed that have been cited at least that many times. All indices include non-research articles such as reviews, editorials, and letters. Google Scholar, which blocks any attempt at data harvesting, indexes an even wider variety of publications (e.g., blog posts, wiki articles), so its value for h-index is generally much higher.\\
\\

\\f0\\b \\cf2 Mendeley Readers 
\\f1\\b0 \\cf2 counts the number of people who have saved a particular article to that bibliographic management tool. A
\\'a0{\\field{\\*\\fldinst{HYPERLINK \"http://dx.doi.org/10.17632/yvbj3rrb49.1\"}}{\\fldrslt \\cf3 2018 study}} showed that this was a leading indicator for the citation count a paper will receive.}";

/*
## \\f0\\b \\cf2 Mendeley Percentile 
\\f1\\b0 \\cf2 is a reflection of how that count compares to other Weill Cornell Medicine publications also authored in the past two years."
*/

## Combine everything. Leaving this hear to troubleshoot when one of the fields is broken!


/*
select 

@top5PercentileAll,
@top10PercentileAll,
@top5DenominatorAll,
@facultyRank,
@top10DenominatorAll,
@nameLast,
@top5RankAll,
@top10RankAll,

@8_summary_bullet5,


@1_before_title,
@2_h1_header,
@3_intro_paragraph,
@4_summary_bullet1,

case 
when @countFirst + @countSenior >= 10 then @5_summary_bullet2
when @countFirst + @countSenior >= 5 then @5_summary_bullet2a
else @5_summary_bullet2b
end,

case 
when @countSenior >= 10 then @6_summary_bullet3
when @countSenior >= 5 then @6_summary_bullet3a
else @6_summary_bullet3b
end,

case 
when @countFirst >= 10 then @7_summary_bullet4
when @countFirst >= 5 then @7_summary_bullet4a
else @7_summary_bullet4b
end, 


case 
when @countAll >= 10 then @8_summary_bullet5
when @countAll >= 5 then @8_summary_bullet5a
else @8_summary_bullet5b
end, 

@10_influence_intro,
@10a_influence_first_last_intro,
case 
when @11_influence_first_last is not null then @11_influence_first_last
else ''
end,
@12_influence_any_position,
@13_influence_all,
@14_recent_intro,
case 
when @15_recent_articles is not null then @15_recent_articles
else ''
end,
case 
when @countOlder > 0 then @16_older_intro
else ''
end,
case 
when @16a_older_articles is not null then @16a_older_articles
else ''
end,
@19_additional_stats,
@20_ending;

*/






select 
concat(
@1_before_title,
@2_h1_header,
@3_intro_paragraph,
@4_summary_bullet1,

case 
when @countFirst + @countSenior >= 10 then @5_summary_bullet2
when @countFirst + @countSenior >= 5 then @5_summary_bullet2a
else @5_summary_bullet2b
end,

case 
when @countSenior >= 10 then @6_summary_bullet3
when @countSenior >= 5 then @6_summary_bullet3a
else @6_summary_bullet3b
end,

case 
when @countFirst >= 10 then @7_summary_bullet4
when @countFirst >= 5 then @7_summary_bullet4a
else @7_summary_bullet4b
end, 

case 
when @countAll >= 10 then @8_summary_bullet5
when @countAll >= 5 then @8_summary_bullet5a
else @8_summary_bullet5b
end, 

@10_influence_intro,
@10a_influence_first_last_intro,
case 
when @11_influence_first_last is not null then @11_influence_first_last
else ''
end,
@12_influence_any_position,
@13_influence_all,
@14_recent_intro,
case 
when @15_recent_articles is not null then @15_recent_articles
else ''
end,
case 
when @countOlder > 0 then @16_older_intro
else ''
end,
case 
when @16a_older_articles is not null then @16a_older_articles
else ''
end,
@19_additional_stats,
@20_ending) as x;


END;
////
DELIMITER ;


DELIMITER ////
CREATE DEFINER=`admin`@`%` PROCEDURE `generatePubsNoPeopleRTF`(
    IN pmidArray mediumblob
)
BEGIN

## Truncate temporary tables 

truncate analysis_temp_output_author;
truncate analysis_temp_output_article;


## Populate a temporary author table with known pmid and personIdentifier values 

INSERT into analysis_temp_output_author(pmid, personIdentifier)
SELECT pmid, personIdentifier
FROM analysis_summary_author 
WHERE FIND_IN_SET(pmid, pmidArray);


## Populate the temporary article table. 
## FYI: the reason we can't just use analysis_summary_article is to account for the case that multiple authors could be bolded. 


INSERT into analysis_temp_output_article (pmid, authors)
SELECT 
y.pmid,
case
when totalAuthorCount < 8 then authors
else 
concat(
SUBSTRING_INDEX(authors,',',6),
' ...',
SUBSTRING_INDEX(authors,',',-1) 
)
end as authors
from (select distinct
personIdentifier,
pmid, 
max(rank) as totalAuthorCount,
group_concat(authorName order by rank asc SEPARATOR ', ') as authors
from 
(
select 
personIdentifier, pmid, rank, min(authorName) as authorName
from 
(select 
distinct
aa.personIdentifier,
aa.pmid,
rank,
cast(concat(authorLastName,' ', authorFirstName) as char)
as authorName
from person_article_author aa
join person_article a on a.pmid = aa.pmid and a.personIdentifier = aa.personIdentifier
join analysis_temp_output_author a1 on a1.pmid = aa.pmid and a1.personIdentifier = aa.personIdentifier
where userAssertion = 'ACCEPTED') m
group by pmid, rank
order by pmid, rank

) x
group by pmid, personIdentifier) y;



## Update a field that has an RTF-friendly equivalent of authors

update analysis_temp_output_article a 
join analysis_summary_author a1 on a1.pmid = a.pmid
set a.authorsRTF = a1.authorsRTF
where a.authors = a1.authors
and a.authors not like '%((%((%';


update analysis_temp_output_article 
set authorsRTF = authors
where authorsRTF is null;


update analysis_temp_output_article a 
inner join analysis_summary_author s on s.pmid = a.pmid 
set specialCharacterFixNeeded = 1
where char_length(a.authors) = char_length(s.authors);


update analysis_temp_output_article a 
inner join analysis_summary_author s on s.pmid = a.pmid 
set specialCharacterFixNeeded = 1
where s.authorsRTF not like '%\\%';



## Replace special characters when specialCharacterFixNeeded = 0

SET @id = 0;

REPEAT 

   SET @id = @id + 1; 

     select specialCharacter, RTFescape 
     into @specialCharacter, @RTFescape
     from analysis_special_characters 
     where id = @id;

     update analysis_temp_output_article
     set authorsRTF = REPLACE(authorsRTF, @specialCharacter, @RTFescape)
     where authorsRTF like(concat('%',@specialCharacter,'%'))
     and specialCharacterFixNeeded = 1 ;

   UNTIL @id = (select max(id) from analysis_special_characters)
END REPEAT;





set @2_article_list = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by datePublicationAddedToEntrez desc),
". ",
case 
  when a.authors like "((%" then replace(replace(authorsRTF,"((","
\\f0\\b "),"))","
\\f1\\b0 ")
  when a.authors like "%((%" then concat("",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0 "))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f3 .
\\
\\
"
) as y

from analysis_temp_output_article a 
join analysis_summary_article a1 on a1.pmid = a.pmid  
order by datePublicationAddedToEntrez desc
) x
);





## Output the surrounding RTF syntax

set @1_before_title = "{\\rtf1\\ansi\\ansicpg1252\\cocoartf2580
\\cocoatextscaling0\\cocoaplatform0{\\fonttbl\\f0\\fswiss\\fcharset0 Arial-BoldMT;\\f1\\fswiss\\fcharset0 ArialMT;\\f2\\fswiss\\fcharset0 Helvetica;
\\f3\\fswiss\\fcharset0 Arial-ItalicMT;\\f4\\froman\\fcharset0 TimesNewRomanPSMT;\\f5\\fnil\\fcharset128 MS-Gothic;
}
{\\colortbl;\\red255\\green255\\blue255;\\red38\\green38\\blue38;\\red42\\green93\\blue160;\\red5\\green99\\blue193;
}
{\\*\\expandedcolortbl;;\\csgenericrgb\\c14902\\c14902\\c14902;\\csgenericrgb\\c16471\\c36471\\c62745;\\csgenericrgb\\c1961\\c38824\\c75686;
}
{\\*\\listtable{\\list\\listtemplateid1\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid1\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listname ;}\\listid1}
{\\list\\listtemplateid2\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid101\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid102\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid2}
{\\list\\listtemplateid3\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid201\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid202\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid3}
{\\list\\listtemplateid4\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid301\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid302\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid4}}
{\\*\\listoverridetable{\\listoverride\\listid1\\listoverridecount0\\ls1}{\\listoverride\\listid2\\listoverridecount0\\ls2}{\\listoverride\\listid3\\listoverridecount0\\ls3}{\\listoverride\\listid4\\listoverridecount0\\ls4}}
\\margl1440\\margr1440\\vieww23220\\viewh13800\\viewkind1\\viewscale150
\\deftab720
\\pard\\tx720\\tx1440\\tx2160\\tx2880\\tx3600\\tx4320\\tx5040\\tx5760\\tx6480\\tx7200\\tx7920\\tx8640\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0";


select 
concat(
@1_before_title,
@2_article_list,
'}'
) as x;


END;
////
DELIMITER ;


DELIMITER ////
CREATE DEFINER=`admin`@`%` PROCEDURE `generatePubsPeopleOnlyRTF`(
    IN personIdentifierArray mediumblob
)
BEGIN

## Truncate temporary tables 

truncate analysis_temp_output_author;
truncate analysis_temp_output_article;


## Populate a temporary author table with known pmid and personIdentifier values 

INSERT into analysis_temp_output_author(pmid, personIdentifier)
SELECT pmid, personIdentifier
FROM analysis_summary_author 
WHERE FIND_IN_SET(personIdentifier, personIdentifierArray);


## Populate the temporary article table. 
## FYI: the reason we can't just use analysis_summary_article is to account for the case that multiple authors could be bolded. 


INSERT into analysis_temp_output_article (pmid, authors)
SELECT 
y.pmid,
case
when totalAuthorCount < 8 then authors
else 
concat(
SUBSTRING_INDEX(authors,',',6),
' ...',
SUBSTRING_INDEX(authors,',',-1) 
)
end as authors
from (select distinct
personIdentifier,
pmid, 
max(rank) as totalAuthorCount,
group_concat(authorName order by rank asc SEPARATOR ', ') as authors
from 
(
select 
personIdentifier, pmid, rank, min(authorName) as authorName
from 
(select 
distinct
aa.personIdentifier,
aa.pmid,
rank,
convert(  
case  
when targetAuthor = 1 then concat('((',authorLastName,' ',replace(cast(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') as char),' ',''),'))') 
else concat(authorLastName,' ',replace(cast(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') as char),' ','')) 
end using utf8) as authorName
from person_article_author aa
join person_article a on a.pmid = aa.pmid and a.personIdentifier = aa.personIdentifier
join analysis_temp_output_author a1 on a1.pmid = aa.pmid and a1.personIdentifier = aa.personIdentifier
where userAssertion = 'ACCEPTED') m
group by pmid, rank
order by pmid, rank

) x
group by pmid, personIdentifier) y;



## Update a field that has an RTF-friendly equivalent of authors

update analysis_temp_output_article a 
join analysis_summary_author a1 on a1.pmid = a.pmid
set a.authorsRTF = a1.authorsRTF
where a.authors = a1.authors
and a.authors not like '%((%((%';


update analysis_temp_output_article 
set authorsRTF = authors
where authorsRTF is null;



## Flag the rare case where there's more than one target author 
## and a need for the RTF special character fix.

/*
update analysis_temp_output_article a 
inner join analysis_summary_author s on s.pmid = a.pmid 
set specialCharacterFixNeeded = 1
where char_length(a.authors) = char_length(s.authors);

update analysis_temp_output_article a 
inner join analysis_summary_author s on s.pmid = a.pmid 
set specialCharacterFixNeeded = 1
where s.authorsRTF not like '%\\%';
*/

update analysis_temp_output_article a 
set specialCharacterFixNeeded = 1
where authors like '%((%((%';



## Replace special characters when specialCharacterFixNeeded = 0

SET @id = 0;

REPEAT 

   SET @id = @id + 1; 

     select specialCharacter, RTFescape 
     into @specialCharacter, @RTFescape
     from analysis_special_characters 
     where id = @id;

     update analysis_temp_output_article
     set authorsRTF = REPLACE(authorsRTF, @specialCharacter, @RTFescape)
     where authorsRTF like(concat('%',@specialCharacter,'%'))
     and specialCharacterFixNeeded = 1 ;

   UNTIL @id = (select max(id) from analysis_special_characters)
END REPEAT;





set @2_article_list = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by datePublicationAddedToEntrez desc),
". ",
case 
  when a.authors like "((%" then replace(replace(authorsRTF,"((","
\\f0\\b "),"))","
\\f1\\b0 ")
  when a.authors like "%((%" then concat("",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0 "))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f3 .
\\
\\
"
) as y

from analysis_temp_output_article a 
join analysis_summary_article a1 on a1.pmid = a.pmid  
order by datePublicationAddedToEntrez desc
) x
);





## Output the surrounding RTF syntax

set @1_before_title = "{\\rtf1\\ansi\\ansicpg1252\\cocoartf2580
\\cocoatextscaling0\\cocoaplatform0{\\fonttbl\\f0\\fswiss\\fcharset0 Arial-BoldMT;\\f1\\fswiss\\fcharset0 ArialMT;\\f2\\fswiss\\fcharset0 Helvetica;
\\f3\\fswiss\\fcharset0 Arial-ItalicMT;\\f4\\froman\\fcharset0 TimesNewRomanPSMT;\\f5\\fnil\\fcharset128 MS-Gothic;
}
{\\colortbl;\\red255\\green255\\blue255;\\red38\\green38\\blue38;\\red42\\green93\\blue160;\\red5\\green99\\blue193;
}
{\\*\\expandedcolortbl;;\\csgenericrgb\\c14902\\c14902\\c14902;\\csgenericrgb\\c16471\\c36471\\c62745;\\csgenericrgb\\c1961\\c38824\\c75686;
}
{\\*\\listtable{\\list\\listtemplateid1\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid1\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listname ;}\\listid1}
{\\list\\listtemplateid2\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid101\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid102\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid2}
{\\list\\listtemplateid3\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid201\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid202\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid3}
{\\list\\listtemplateid4\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid301\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid302\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid4}}
{\\*\\listoverridetable{\\listoverride\\listid1\\listoverridecount0\\ls1}{\\listoverride\\listid2\\listoverridecount0\\ls2}{\\listoverride\\listid3\\listoverridecount0\\ls3}{\\listoverride\\listid4\\listoverridecount0\\ls4}}
\\margl1440\\margr1440\\vieww23220\\viewh13800\\viewkind1\\viewscale150
\\deftab720
\\pard\\tx720\\tx1440\\tx2160\\tx2880\\tx3600\\tx4320\\tx5040\\tx5760\\tx6480\\tx7200\\tx7920\\tx8640\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0";


select 

concat(
@1_before_title,
@2_article_list,
'
}'
) 
as x;

END;
////
DELIMITER ;


DELIMITER ////
CREATE DEFINER=`admin`@`%` PROCEDURE `generatePubsRTF`(
    IN personIdentifierArray mediumblob,
    IN pmidArray mediumblob
)
BEGIN

## Truncate temporary tables 

truncate analysis_temp_output_author;
truncate analysis_temp_output_article;


## Populate a temporary author table with known pmid and personIdentifier values 

INSERT into analysis_temp_output_author(pmid, personIdentifier)
SELECT pmid, personIdentifier
FROM analysis_summary_author 
WHERE FIND_IN_SET(personIdentifier, personIdentifierArray)
and FIND_IN_SET(pmid, pmidArray);


## Populate the temporary article table. 
## FYI: the reason we can't just use analysis_summary_article is to account for the case that multiple authors could be bolded. 


INSERT into analysis_temp_output_article (pmid, authors)
SELECT 
y.pmid,
case
when totalAuthorCount < 8 then authors
else 
concat(
SUBSTRING_INDEX(authors,',',6),
' ...',
SUBSTRING_INDEX(authors,',',-1) 
)
end as authors
from (select distinct
personIdentifier,
pmid, 
max(rank) as totalAuthorCount,
group_concat(authorName order by rank asc SEPARATOR ', ') as authors
from 
(
select 
personIdentifier, pmid, rank, min(authorName) as authorName
from 
(select 
distinct
aa.personIdentifier,
aa.pmid,
rank,
convert(  
case  
when targetAuthor = 1 then concat('((',authorLastName,' ',replace(cast(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') as char),' ',''),'))') 
else concat(authorLastName,' ',replace(cast(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') as char),' ','')) 
end   
using utf8) as authorName
from person_article_author aa
join person_article a on a.pmid = aa.pmid and a.personIdentifier = aa.personIdentifier
join analysis_temp_output_author a1 on a1.pmid = aa.pmid and a1.personIdentifier = aa.personIdentifier
where userAssertion = 'ACCEPTED') m
group by pmid, rank
order by pmid, rank

) x
group by pmid, personIdentifier) y;



## Update a field that has an RTF-friendly equivalent of authors

update analysis_temp_output_article a 
join analysis_summary_author a1 on a1.pmid = a.pmid
set a.authorsRTF = a1.authorsRTF
where a.authors = a1.authors
and a.authors not like '%((%((%';


update analysis_temp_output_article 
set authorsRTF = authors
where authorsRTF is null;



## Flag the rare case where there's more than one target author 
## and a need for the RTF special character fix.

/*
update analysis_temp_output_article a 
inner join analysis_summary_author s on s.pmid = a.pmid 
set specialCharacterFixNeeded = 1
where char_length(a.authors) = char_length(s.authors);

update analysis_temp_output_article a 
inner join analysis_summary_author s on s.pmid = a.pmid 
set specialCharacterFixNeeded = 1
where s.authorsRTF not like '%\\%';
*/

update analysis_temp_output_article a 
set specialCharacterFixNeeded = 1
where authors like '%((%((%';
# and authorsRTF like '%\\\\\'%';


/*
select * from 
analysis_temp_output_article 
where authors like '%((%(%' 
# and authorsRTF like "%e9%"
*/



## Use already updated authors RTF special characters from analysis_summary_article

/*
update analysis_temp_output_article a 
join analysis_summary_author a2 on a2.pmid = a.pmid  
join analysis_temp_output_author a1 on a1.pmid = a.pmid and a1.personIdentifier = a2.personIdentifier
set a.authorsRTF = a2.authorsRTF
where specialCharacterFixNeeded = 1;
*/



## Replace special characters when specialCharacterFixNeeded = 0

SET @id = 0;

REPEAT 

   SET @id = @id + 1; 

     select specialCharacter, RTFescape 
     into @specialCharacter, @RTFescape
     from analysis_special_characters 
     where id = @id;

     update analysis_temp_output_article
     set authorsRTF = REPLACE(authorsRTF, @specialCharacter, @RTFescape)
     where authorsRTF like(concat('%',@specialCharacter,'%'))
     and specialCharacterFixNeeded = 1 ;

   UNTIL @id = (select max(id) from analysis_special_characters)
END REPEAT;





set @2_article_list = (select 
group_concat(y separator '') from
(select 
concat("
\\f1\\b0\\fs22 \\cf2 ",
row_number() over (order by datePublicationAddedToEntrez desc),
". ",
case 
  when a.authors like "((%" then replace(replace(authorsRTF,"((","
\\f0\\b "),"))","
\\f1\\b0 ")
  when a.authors like "%((%" then concat("",replace(replace(authorsRTF," ((","\\'a0
\\f0\\b "),"))","
\\f1\\b0 "))
  else authorsRTF 
end,
". ",
articleTitleRTF,
"\\'a0
\\f3\\i ",
journalTitleVerbose,
"
\\f1\\i0. ",
if(articleYear != 0, articleYear, left(publicationDateDisplay,4)),
case 
  when volume is not null and pages is not null then concat(";",volume,":",pages)
  when pages is not null then concat(";",pages)
  else ""
end,
". PMID: ",
a.pmid,
".",
"\\'a0{\\field{\\*\\fldinst{HYPERLINK \"",
case 
  when pmcid is not null and pmcid != '' then concat('https://www.ncbi.nlm.nih.gov/pmc/articles/',pmcid,'/')
  when a1.doi is not null and a1.doi != '' then concat('https://dx.doi.org/',a1.doi)
  when a.pmid is not null then concat('https://pubmed.ncbi.nlm.nih.gov/',a.pmid)
end,
"\"}}{\\fldrslt \\cf3 Full text}}
\\f3 .
\\
\\
"
) as y

from analysis_temp_output_article a 
join analysis_summary_article a1 on a1.pmid = a.pmid  
order by datePublicationAddedToEntrez desc
) x
);





## Output the surrounding RTF syntax

set @1_before_title = "{\\rtf1\\ansi\\ansicpg1252\\cocoartf2580
\\cocoatextscaling0\\cocoaplatform0{\\fonttbl\\f0\\fswiss\\fcharset0 Arial-BoldMT;\\f1\\fswiss\\fcharset0 ArialMT;\\f2\\fswiss\\fcharset0 Helvetica;
\\f3\\fswiss\\fcharset0 Arial-ItalicMT;\\f4\\froman\\fcharset0 TimesNewRomanPSMT;\\f5\\fnil\\fcharset128 MS-Gothic;
}
{\\colortbl;\\red255\\green255\\blue255;\\red38\\green38\\blue38;\\red42\\green93\\blue160;\\red5\\green99\\blue193;
}
{\\*\\expandedcolortbl;;\\csgenericrgb\\c14902\\c14902\\c14902;\\csgenericrgb\\c16471\\c36471\\c62745;\\csgenericrgb\\c1961\\c38824\\c75686;
}
{\\*\\listtable{\\list\\listtemplateid1\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid1\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listname ;}\\listid1}
{\\list\\listtemplateid2\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid101\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid102\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid2}
{\\list\\listtemplateid3\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid201\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid202\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid3}
{\\list\\listtemplateid4\\listhybrid{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid301\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li720\\lin720 }{\\listlevel\\levelnfc23\\levelnfcn23\\leveljc0\\leveljcn0\\levelfollow0\\levelstartat1\\levelspace360\\levelindent0{\\*\\levelmarker \\{disc\\}}{\\leveltext\\leveltemplateid302\\'01\\uc0\\u8226 ;}{\\levelnumbers;}\\fi-360\\li1440\\lin1440 }{\\listname ;}\\listid4}}
{\\*\\listoverridetable{\\listoverride\\listid1\\listoverridecount0\\ls1}{\\listoverride\\listid2\\listoverridecount0\\ls2}{\\listoverride\\listid3\\listoverridecount0\\ls3}{\\listoverride\\listid4\\listoverridecount0\\ls4}}
\\margl1440\\margr1440\\vieww23220\\viewh13800\\viewkind1\\viewscale150
\\deftab720
\\pard\\tx720\\tx1440\\tx2160\\tx2880\\tx3600\\tx4320\\tx5040\\tx5760\\tx6480\\tx7200\\tx7920\\tx8640\\pardeftab720\\ri0\\sl264\\slmult1\\sa20\\partightenfactor0";


select 
concat(
@1_before_title,
@2_article_list,
'}'
) as x;


END;
////
DELIMITER ;


DELIMITER //
CREATE DEFINER=`admin`@`%` PROCEDURE `populateAnalysisSummaryPersonScopeTable`()
begin
truncate analysis_summary_person_scope;

insert into analysis_summary_person_scope (personIdentifier)
select distinct personIdentifier
from person_person_type
where personType = 'academic-faculty-weillfulltime';
end;
//
DELIMITER ;


DELIMITER //
CREATE DEFINER=`admin`@`%` PROCEDURE `populateAnalysisSummaryTables`()
BEGIN

## With this stored procedure, we run a set of background jobs for summarizing all the publication
## data used in the ReCiter Publication Manager web interface and the report generator.

## Our goal here is to populate three tables:
## 1. analysis_summary_author - authorship-level data
## 2. analysis_summary_article - article-level data
## 3. analysis_summary_person - person-level data

## In WCM's experience, this procedure takes ~17 minutes to complete while running on AWS RDS, MariaDB 10.6
## and with about 300,000 accepted articles. 

## The below is a hedge just to make sure this procedure doesn't start 
## unless the person_article table is populated with at least some articles.

IF ((select count(*) from person_article) > 5) THEN


#### 1. Start from scratch ####

truncate analysis_summary_author;
truncate analysis_summary_article;
truncate analysis_summary_person;
truncate analysis_summary_author_list;



#### 2a. Populate "analysis_summary_author" table with known authors #### 

insert into analysis_summary_author (pmid, personIdentifier, authorPosition, authors) 
select  
y.pmid, 
y.personIdentifier, 
case  
when authors like '((%' then 'first'  
when authors like '%))' then 'last' 
end as authorPosition,  
case  
when totalAuthorCount < 8 then authors  
else  
concat( 
SUBSTRING_INDEX(authors,',',6), 
' ...', 
SUBSTRING_INDEX(authors,',',-1)   
) 
end as authors  
    
from (select distinct 
personIdentifier, 
pmid,   
max(rank) as totalAuthorCount,  
group_concat(authorName order by rank asc SEPARATOR ', ') as authors  
from  
( 
select  
distinct  
aa.personIdentifier,  
aa.pmid,  
rank, 
convert(  
case  
when targetAuthor = 1 then concat('((',authorLastName,' ',replace(cast(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') as char),' ',''),'))') 
else concat(authorLastName,' ',replace(cast(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') as char),' ','')) 
end   
using utf8) 
as authorName 
from person_article_author aa 
join person_article a on a.pmid = aa.pmid and a.personIdentifier = aa.personIdentifier 
where userAssertion = 'ACCEPTED'  
) x 
group by pmid, personIdentifier) y; 

update analysis_summary_author a  
join analysis_override_author_position o on a.pmid = o.pmid and a.personIdentifier = o.personIdentifier 
set a.authorPosition = o.position;



#### 2b. Update analysis_summary_author with cases where authors are marked as equalContrib relative to first/last authors
## To receive credit for authorPosition = first or last, authors need to be contiguous with other authors who
## also have the equalContrib designation.
##
## For some reason, this doesn't work without ta temporary table.



CREATE TABLE if not exists `analysis_temp_equalcontrib` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(30) DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `rank` int(11) DEFAULT NULL,
  `maxRank` int(11) DEFAULT NULL,  
  `targetAuthor` int(11) DEFAULT NULL,    
  `equalContribAll` varchar(500) DEFAULT NULL,      
  `authorPositionEqualContrib` varchar(20) DEFAULT NULL,        
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`) USING BTREE,
  KEY `pmid` (`pmid`) USING BTREE  
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4
  COLLATE utf8mb4_unicode_ci;



INSERT INTO analysis_temp_equalcontrib (personIdentifier, pmid, rank, maxRank, targetAuthor, equalContribAll, authorPositionEqualContrib)

SELECT a.personIdentifier as personIdentifier,
     a.pmid as pmid,
     a.rank,
     maxRank,
     a.targetAuthor,
     equalContribAll,
       CASE
           -- Contiguous to 1
           WHEN a.rank = 2 
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
           THEN 'first'

           WHEN a.rank = 3
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
           THEN 'first'

           WHEN a.rank = 4
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
           THEN 'first'

           WHEN a.rank = 5
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0               
           THEN 'first'

           WHEN a.rank = 6
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0                             
           THEN 'first'

           WHEN a.rank = 7
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0                                           
           THEN 'first'

           WHEN a.rank = 8
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0                                                         
           THEN 'first'

           WHEN a.rank = 9
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0                                                                         
           THEN 'first'

           WHEN a.rank = 10
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0                                                                         
              AND FIND_IN_SET(10, equalContribAll) > 0    
           THEN 'first'

           WHEN a.rank = 11
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0     
              AND FIND_IN_SET(10, equalContribAll) > 0  
              AND FIND_IN_SET(11, equalContribAll) > 0                                                                                                    
           THEN 'first'

           WHEN a.rank = 12
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0     
              AND FIND_IN_SET(10, equalContribAll) > 0  
              AND FIND_IN_SET(11, equalContribAll) > 0                                                                                                    
              AND FIND_IN_SET(12, equalContribAll) > 0   
           THEN 'first'

           WHEN a.rank = 13
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0     
              AND FIND_IN_SET(10, equalContribAll) > 0  
              AND FIND_IN_SET(11, equalContribAll) > 0                                                                                                    
              AND FIND_IN_SET(12, equalContribAll) > 0   
              AND FIND_IN_SET(13, equalContribAll) > 0 
           THEN 'first'

           WHEN a.rank = 14
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0     
              AND FIND_IN_SET(10, equalContribAll) > 0  
              AND FIND_IN_SET(11, equalContribAll) > 0                                                                                                    
              AND FIND_IN_SET(12, equalContribAll) > 0   
              AND FIND_IN_SET(13, equalContribAll) > 0 
              AND FIND_IN_SET(14, equalContribAll) > 0                
           THEN 'first'

           WHEN a.rank = 15
              AND FIND_IN_SET(1, equalContribAll) > 0  
              AND FIND_IN_SET(2, equalContribAll) > 0
              AND FIND_IN_SET(3, equalContribAll) > 0
              AND FIND_IN_SET(4, equalContribAll) > 0
              AND FIND_IN_SET(5, equalContribAll) > 0    
              AND FIND_IN_SET(6, equalContribAll) > 0   
              AND FIND_IN_SET(7, equalContribAll) > 0   
              AND FIND_IN_SET(8, equalContribAll) > 0  
              AND FIND_IN_SET(9, equalContribAll) > 0     
              AND FIND_IN_SET(10, equalContribAll) > 0  
              AND FIND_IN_SET(11, equalContribAll) > 0                                                                                                    
              AND FIND_IN_SET(12, equalContribAll) > 0   
              AND FIND_IN_SET(13, equalContribAll) > 0 
              AND FIND_IN_SET(14, equalContribAll) > 0   
              AND FIND_IN_SET(15, equalContribAll) > 0                              
           THEN 'first'

           WHEN a.rank = maxRank - 1
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
           THEN 'last'

           WHEN a.rank = maxRank - 2
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0               
           THEN 'last'

           WHEN a.rank = maxRank - 3
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0                             
           THEN 'last'

           WHEN a.rank = maxRank - 4
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
           THEN 'last'

           WHEN a.rank = maxRank - 5
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
           THEN 'last'

           WHEN a.rank = maxRank - 6
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
           THEN 'last'

           WHEN a.rank = maxRank - 7
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
           THEN 'last'

           WHEN a.rank = maxRank - 8
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
           THEN 'last'

           WHEN a.rank = maxRank - 9
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
           THEN 'last'

           WHEN a.rank = maxRank - 10
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
           THEN 'last'

           WHEN a.rank = maxRank - 11
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
              AND FIND_IN_SET(maxRank - 11, equalContribAll) > 0             
           THEN 'last'

           WHEN a.rank = maxRank - 12
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
              AND FIND_IN_SET(maxRank - 11, equalContribAll) > 0             
              AND FIND_IN_SET(maxRank - 12, equalContribAll) > 0 
           THEN 'last'

           WHEN a.rank = maxRank - 13
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
              AND FIND_IN_SET(maxRank - 11, equalContribAll) > 0             
              AND FIND_IN_SET(maxRank - 12, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 13, equalContribAll) > 0                
           THEN 'last'

           WHEN a.rank = maxRank - 13
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
              AND FIND_IN_SET(maxRank - 11, equalContribAll) > 0             
              AND FIND_IN_SET(maxRank - 12, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 13, equalContribAll) > 0                
           THEN 'last'

           WHEN a.rank = maxRank - 14
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
              AND FIND_IN_SET(maxRank - 11, equalContribAll) > 0             
              AND FIND_IN_SET(maxRank - 12, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 13, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 14, equalContribAll) > 0                              
           THEN 'last'

           WHEN a.rank = maxRank - 15
              AND FIND_IN_SET(maxRank, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 1, equalContribAll) > 0
              AND FIND_IN_SET(maxRank - 2, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 3, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 4, equalContribAll) > 0                                           
              AND FIND_IN_SET(maxRank - 5, equalContribAll) > 0   
              AND FIND_IN_SET(maxRank - 6, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 7, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 8, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 9, equalContribAll) > 0               
              AND FIND_IN_SET(maxRank - 10, equalContribAll) > 0    
              AND FIND_IN_SET(maxRank - 11, equalContribAll) > 0             
              AND FIND_IN_SET(maxRank - 12, equalContribAll) > 0 
              AND FIND_IN_SET(maxRank - 13, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 14, equalContribAll) > 0  
              AND FIND_IN_SET(maxRank - 15, equalContribAll) > 0                                            
           THEN 'last'
           ELSE NULL
       END AS authorPositionEqualContrib
FROM person_article_author a
join person_article p on p.pmid = a.pmid and p.personIdentifier = a.personIdentifier
LEFT JOIN analysis_summary_author r ON r.pmid = a.pmid AND r.personIdentifier = a.personIdentifier
JOIN (SELECT pmid, max(rank) AS maxRank
      FROM person_article_author
      GROUP BY pmid) m ON m.pmid = a.pmid
JOIN (SELECT pmid, GROUP_CONCAT(DISTINCT rank ORDER BY rank ASC SEPARATOR ',') AS equalContribAll
      FROM person_article_author
      WHERE equalContrib = 'Y'
      GROUP BY pmid) y ON y.pmid = a.pmid
WHERE a.equalContrib = 'Y'
  AND a.targetAuthor = 1
  and p.userAssertion = 'ACCEPTED';
  
  
update analysis_summary_author y
join analysis_temp_equalcontrib x on x.pmid = y.pmid and x.personIdentifier = y.personIdentifier
set y.authorPosition = x.authorPositionEqualContrib
where x.authorPositionEqualContrib is not null and y.authorPosition is null;







#### 2c.  Populate "analysis_summary_author_list" table with all authors and their ranks ####


insert into analysis_summary_author_list (pmid, authorFirstName, authorLastName, rank, personIdentifier)

select pmid, max(authorFirstName) as authorFirstName, max(authorLastName) as authorLastName, rank, max(personIdentifier) as personIdentifier

from 

(select
aa.personIdentifier,
aa.pmid,
authorFirstName,
authorLastName,
rank,
targetAuthor
from person_article_author aa 
join person_article a on a.pmid = aa.pmid and a.personIdentifier = aa.personIdentifier 
where userAssertion = 'ACCEPTED'
and targetAuthor = 1

union 

select
'' as personIdentifier,
pmid,
authorFirstName,
authorLastName,
rank,
targetAuthor
from person_article_author
where
targetAuthor = 0) x 
group by pmid, rank
order by pmid desc, rank asc;



#### 3. Populate "analysis_summary_article" table with articles ####

insert into analysis_summary_article (pmid, pmcid, publicationTypeCanonical, articleYear, publicationDateStandardized, publicationDateDisplay, datePublicationAddedToEntrez, articleTitle, journalTitleVerbose, issn, doi, issue, volume, pages, citationCountScopus)
select distinct 
pmid, max(pmcid), publicationTypeCanonical, articleYear, min(publicationDateStandardized), publicationDateDisplay, datePublicationAddedToEntrez, articleTitle, journalTitleVerbose, issn, doi, issue, volume, pages, max(timesCited)
from person_article 
where userAssertion = 'ACCEPTED'
group by pmid
order by datePublicationAddedToEntrez desc;


## Update analysis_summary_article with Scimago Journal Rank (SJR)

update analysis_summary_article a 
join journal_impact_scimago i on i.issn1 = a.issn
set journalImpactScore1 = i.sjr
where a.journalImpactScore1 is null and a.issn is not null;

update analysis_summary_article a 
join journal_impact_scimago i on i.issn2 = a.issn
set journalImpactScore1 = i.sjr
where a.journalImpactScore1 is null and a.issn is not null;

update analysis_summary_article a 
join journal_impact_scimago i on i.issn3 = a.issn
set journalImpactScore1 = i.sjr
where a.journalImpactScore1 is null and a.issn is not null;


## Update analysis_summary_article with an alternate journal ranking scheme in journal_impact_alternative

update analysis_summary_article a 
join journal_impact_alternative i on i.issn = a.issn
set journalImpactScore2 = i.impactScore1
where a.journalImpactScore2 is null and a.issn is not null;

update analysis_summary_article a 
join journal_impact_alternative i on i.eissn = a.issn
set journalImpactScore2 = i.impactScore1
where a.journalImpactScore2 is null and a.issn is not null;


## Update Mendeley readers and TrendingPubs score

update analysis_summary_article a
join analysis_altmetric al on al.doi = a.doi
set a.readersMendeley = al.`readers-mendeley`
where round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24),0) < 366;

update analysis_summary_article 
set trendingPubsScore = round(readersMendeley / round((unix_timestamp() - UNIX_TIMESTAMP(STR_TO_DATE(publicationDateStandardized,'%Y-%m-%d')) ) 
  / (60 * 60 * 24),0),2);


## Update NIH RCR stats

update analysis_summary_article a
join analysis_nih r on r.pmid = a.pmid
set a.citationCountNIH = r.citation_count, 
a.percentileNIH = r.nih_percentile,
a.relativeCitationRatioNIH = r.relative_citation_ratio,
publicationTypeNIH = 
  case when is_research_article in ('Yes','yes','True') then 'Research Article'
  else null
  end;



## Update article year in cases where year is 0. This is due to a quirk in PubMed that hasn't been
## fixed as yet in ReCiter.

update analysis_summary_article a
set articleYear = left(publicationDateStandardized,4)
where articleYear = 0 or articleYear is null;


## Update datePublicationAddedToEntrez where that value is blank.

update analysis_summary_article a
set datePublicationAddedToEntrez = publicationDateStandardized
where datePublicationAddedToEntrez = '' and publicationDateStandardized != '' and publicationDateStandardized is not null;


#### 4. Manage special characters #### 

## Update a field that has an RTF-friendly equivalent of articleTitle

update analysis_summary_article
set articleTitleRTF = articleTitle;

SET @id = 0;

REPEAT 

   SET @id = @id + 1; 

     select specialCharacter, RTFescape 
     into @specialCharacter, @RTFescape
     from analysis_special_characters 
     where id = @id;

     update analysis_summary_article
     set articleTitleRTF = REPLACE(articleTitleRTF, @specialCharacter, @RTFescape)
     where articleTitleRTF like(concat('%',@specialCharacter,'%'));
     
   UNTIL @id = (select max(id) from analysis_special_characters)
END REPEAT;


## Update a field that has an RTF-friendly equivalent of authors

update analysis_summary_author
set authorsRTF = authors;

SET @id = 0;

REPEAT 

   SET @id = @id + 1; 

     select specialCharacter, RTFescape 
     into @specialCharacter, @RTFescape
     from analysis_special_characters 
     where id = @id;

     update analysis_summary_author
     set authorsRTF = REPLACE(authorsRTF, @specialCharacter, @RTFescape)
     where authorsRTF like(concat('%',@specialCharacter,'%'));

   UNTIL @id = (select max(id) from analysis_special_characters)
END REPEAT;




#### 5. Populate the "analysis_summary_person" table with person-level statistics ####

## This function is site-specific, populating all in scope personIdentifiers in the analysis_summary_person_scope
## table.

call populateAnalysisSummaryPersonScopeTable();


## Populate the analysis_summary_person table

insert into analysis_summary_person (personIdentifier, nameFirst, nameMiddle, nameLast, department, facultyRank)

select * from (
select distinct
p.personIdentifier,
firstName as nameFirst,
middleName as nameMiddle,
lastName as nameLast,
primaryOrganizationalUnit as department,

coalesce(a.facultyRank, b.facultyRank, c.facultyRank, d.facultyRank) as facultyRank
from person p 

left join (select personIdentifier, 'Full Professor' as facultyRank 
from person_person_type
where personType = 'academic-faculty-fullprofessor') a 
on a.personIdentifier = p.personIdentifier

left join (select personIdentifier, 'Associate Professor' as facultyRank 
from person_person_type
where personType = 'academic-faculty-associate') b
on b.personIdentifier = p.personIdentifier

left join (select personIdentifier, 'Assistant Professor' as facultyRank 
from person_person_type
where personType = 'academic-faculty-assistant') c
on c.personIdentifier = p.personIdentifier

left join (select personIdentifier, 'Instructor or Lecturer' as facultyRank 
from person_person_type
where personType in ('academic-faculty-instructor','academic-faculty-lecturer')) d
on d.personIdentifier = p.personIdentifier


inner join (select personIdentifier 
from analysis_summary_person_scope) e
on e.personIdentifier = p.personIdentifier

) x where facultyRank is not null; 



## Count, all

update analysis_summary_person p
join (select s.personIdentifier,
count(a1.pmid) as count
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where publicationTypeNIH = 'Research Article' 
and percentileNIH is not null
group by s.personIdentifier) x on x.personIdentifier = p.personIdentifier
set countAll = count;



## Count, first only

update analysis_summary_person p
join (select s.personIdentifier,
count(a1.pmid) as count
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('first')
group by s.personIdentifier) x on x.personIdentifier = p.personIdentifier
set countFirst = count;


## Count, senior only

update analysis_summary_person p
join (select s.personIdentifier,
count(a1.pmid) as count
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('last')
group by s.personIdentifier) x on x.personIdentifier = p.personIdentifier
set countSenior = count;


## Average, Top 10, all

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and countAll > 9) y 
where article_rank < 11
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top10PercentileAll = percentileNIH;




## Average, Top 5, all

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and countAll > 4) y 
where article_rank < 6
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top5PercentileAll = percentileNIH;


## Average, Top 10, senior only

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('last')
and countAll > 9) y 
where article_rank < 11
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top10PercentileSenior = percentileNIH;


## Average, Top 5, senior only

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('last')
and countAll > 9) y 
where article_rank < 6
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top5PercentileSenior = percentileNIH;


## Average, Top 10, first only

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('first')
and countAll > 9) y 
where article_rank < 11
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top10PercentileFirst = percentileNIH;


## Average, Top 5, first 

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('first')
and countAll > 4) y 
where article_rank < 6
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top5PercentileFirst = percentileNIH;


## Average, Top 10, first or last

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('first','last')
and countAll > 9) y 
where article_rank < 11
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top10PercentileFirstSenior = percentileNIH;


## Average, Top 5, first or last

update analysis_summary_person p
join (
select personIdentifier,
round(avg(percentileNIH),3) as percentileNIH,
count(*) as count
from
(select s.personIdentifier,
a.pmid,
percentileNIH,
rank() over (partition by personIdentifier order by percentileNIH desc) as article_rank
from analysis_summary_person s 
join analysis_summary_author a on a.personIdentifier = s.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where percentileNIH is not null
and authorPosition in ('first','last')
and countAll > 4) y 
where article_rank < 6
group by personIdentifier) x on x.personIdentifier = p.personIdentifier
set top5PercentileFirstSenior = percentileNIH;


## Denominator, top 5, senior only

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top5PercentileSenior is not null and countSenior > 4 group by facultyRank) x on x.facultyRank = p.facultyRank
set top5DenominatorSenior = count;


## Denominator, Top 10, senior only

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top10PercentileSenior is not null and countSenior > 9 group by facultyRank) x on x.facultyRank = p.facultyRank
set top10DenominatorSenior = count;


## Denominator, Top 5, first only

update analysis_summary_person p
join (select count(personIdentifier) as count, facultyRank from analysis_summary_person where top5PercentileFirst is not null and countFirst > 4 group by facultyRank) x on x.facultyRank = p.facultyRank
set top5DenominatorFirst = count;


## Denominator, Top 10, first only

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top10PercentileFirst is not null and countFirst > 9 group by facultyRank) x on x.facultyRank = p.facultyRank
set top10DenominatorFirst = count;




## Denominator, Top 5, first or senior 

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top5PercentileFirstSenior is not null and (countFirst + countSenior) > 4 group by facultyRank) x on x.facultyRank = p.facultyRank
set top5DenominatorFirstSenior = count;




## Denominator, Top 10, first or senior

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top10PercentileFirstSenior is not null and (countFirst + countSenior) > 9 group by facultyRank) x on x.facultyRank = p.facultyRank
set top10DenominatorFirstSenior = count;




## Denominator, Top 5, all 

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top5PercentileAll is not null and countAll > 4 group by facultyRank) x on x.facultyRank = p.facultyRank
set top5DenominatorAll = count;


## Denominator, Top 10, all

update analysis_summary_person p
join (select count(*) as count, facultyRank from analysis_summary_person where top10PercentileAll is not null and countAll > 9 group by facultyRank) x on x.facultyRank = p.facultyRank
set top10DenominatorAll = count;


## Rank, top 5, senior only

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top5PercentileSenior desc) as personRank
from analysis_summary_person
where countSenior > 4) x on x.personIdentifier = p.personIdentifier
set top5RankSenior = personRank;


## Rank, Top 10, senior only

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top10PercentileSenior desc) as personRank
from analysis_summary_person
where countSenior > 9) x on x.personIdentifier = p.personIdentifier
set top10RankSenior = personRank;


## Rank, Top 5, first only

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top5PercentileFirst desc) as personRank
from analysis_summary_person
where countFirst > 4) x on x.personIdentifier = p.personIdentifier
set top5RankFirst = personRank;


## Rank, Top 10, first only

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top10PercentileFirst desc) as personRank
from analysis_summary_person
where countFirst > 9) x on x.personIdentifier = p.personIdentifier
set top10RankFirst = personRank;


## Rank, Top 5, first or senior 

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top5PercentileFirstSenior desc) as personRank
from analysis_summary_person
where (countSenior + countFirst) > 4) x on x.personIdentifier = p.personIdentifier
set top5RankFirstSenior = personRank;


## Rank, Top 10, first or senior

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top10PercentileFirstSenior desc) as personRank
from analysis_summary_person
where (countSenior + countFirst) > 4) x on x.personIdentifier = p.personIdentifier
set top10RankFirstSenior = personRank;


## Rank, Top 5, all 

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top5PercentileAll desc) as personRank
from analysis_summary_person
where countAll > 4) x on x.personIdentifier = p.personIdentifier
set top5RankAll = personRank;


## Rank, Top 10, all

update analysis_summary_person p
join (select 
personIdentifier, 
rank() over (partition by facultyRank order by top10PercentileAll desc) as personRank
from analysis_summary_person
where countAll > 9) x on x.personIdentifier = p.personIdentifier
set top10RankAll = personRank;



#### 6. Compute h-index and h5-index

## We have two approaches for computing h-index and h5-index. 
##
## Option 1. Use NIH iCite which takes data from the analysis_nih table and outputs to 
## h-index and h5-index in analysis_summary_person.
##
## Option 2. Use Scopus which is in the person_article table. This is only available if you 
## have an integration with Scopus.
##
## You can comment out the Scopus option (6c and 6d) if you're not using it, or just let it run. 
## This process will complete really quickly if citation count is 0 for all articles.


## 6a. Compute h-index using NIH iCite data 

update analysis_summary_person h  
set hindexStatus = 0;

update analysis_summary_person h  
set hindexStatus = 1
where personIdentifier not in 
(select distinct p.personIdentifier 
from analysis_summary_person p 
join analysis_summary_author a on p.personIdentifier = a.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where citationCountNIH > 0);  

SET @person_identifier = (select personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1);

proc1: REPEAT   

    select personIdentifier into @personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1;
                
    TRUNCATE analysis_temp_hindex;
            
    INSERT INTO analysis_temp_hindex (personIdentifier, citation_count) 
    SELECT s.personIdentifier, citationCountNIH 
    from analysis_summary_author a 
    join analysis_summary_person s on s.personIdentifier = a.personIdentifier
    join analysis_summary_article a1 on a1.pmid = a.pmid
    where s.personIdentifier = @person_identifier and citationCountNIH > 0 
    ORDER BY citationCountNIH desc;


    SET @article_count := (SELECT count(*) from analysis_temp_hindex);
    SET @max_times_cited := (SELECT max(citation_count) from analysis_temp_hindex);
    SET @temp_hindex := (select least(@article_count, @max_times_cited)) + 1;   

    REPEAT SET @temp_hindex = @temp_hindex - 1; 
      UNTIL 
        (@temp_hindex <= (select count(*) from analysis_temp_hindex WHERE citation_count >= @temp_hindex)) OR 
        (@temp_hindex is null) OR
        (@temp_hindex = 0)
    END REPEAT;
    
    UPDATE analysis_summary_person
    SET hindexStatus = 1, hindexNIH = @temp_hindex
    WHERE personIdentifier = @person_identifier;

    SET @person_identifier = (select personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1);
                        
    UNTIL ((select count(*) from analysis_summary_person WHERE hindexStatus = 0) = 0)  
         
  END REPEAT proc1;
  
  


## 6b. Compute h5-index using NIH iCite data 

update analysis_summary_person h  
set hindexStatus = 0;

update analysis_summary_person h  
set hindexStatus = 1
where personIdentifier not in
(select distinct p.personIdentifier 
from analysis_summary_person p 
join analysis_summary_author a on p.personIdentifier = a.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where citationCountNIH > 0 
and datePublicationAddedToEntrez > CURDATE() - INTERVAL 5 YEAR);  

proc1: REPEAT   

    select personIdentifier into @personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1;
                
    TRUNCATE analysis_temp_hindex;
            
    INSERT INTO analysis_temp_hindex (personIdentifier, citation_count) 
    SELECT s.personIdentifier, citationCountNIH 
    from analysis_summary_author a 
    join analysis_summary_person s on s.personIdentifier = a.personIdentifier
    join analysis_summary_article a1 on a1.pmid = a.pmid
    where s.personIdentifier = @person_identifier 
    and citationCountNIH > 0 
    and datePublicationAddedToEntrez > CURDATE() - INTERVAL 5 YEAR 
    ORDER BY citationCountNIH desc;

    SET @article_count := (SELECT count(*) from analysis_temp_hindex);
    SET @max_times_cited := (SELECT max(citation_count) from analysis_temp_hindex);
    SET @temp_hindex := (select least(@article_count, @max_times_cited)) + 1;   

    REPEAT SET @temp_hindex = @temp_hindex - 1; 
      UNTIL 
        (@temp_hindex <= (select count(*) from analysis_temp_hindex WHERE citation_count >= @temp_hindex)) OR 
        (@temp_hindex is null) OR
        (@temp_hindex = 0)
    END REPEAT;
    
    UPDATE analysis_summary_person
    SET hindexStatus = 1, h5indexNIH = @temp_hindex
    WHERE personIdentifier = @person_identifier;

    SET @person_identifier = (select personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1);
                        
    UNTIL ((select count(*) from analysis_summary_person WHERE hindexStatus = 0) = 0)  
         
  END REPEAT proc1;




## 6c. Compute h-index using Scopus data 

update analysis_summary_person h  
set hindexStatus = 0;

update analysis_summary_person h  
set hindexStatus = 1
where personIdentifier not in 
(select distinct p.personIdentifier 
from analysis_summary_person p 
join analysis_summary_author a on p.personIdentifier = a.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where citationCountScopus > 0);  

SET @person_identifier = (select personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1);

proc1: REPEAT   

    select personIdentifier into @personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1;
                
    TRUNCATE analysis_temp_hindex;
            
    INSERT INTO analysis_temp_hindex (personIdentifier, citation_count) 
    SELECT s.personIdentifier, citationCountScopus 
    from analysis_summary_author a 
    join analysis_summary_person s on s.personIdentifier = a.personIdentifier
    join analysis_summary_article a1 on a1.pmid = a.pmid
    where s.personIdentifier = @person_identifier and citationCountScopus > 0 
    ORDER BY citationCountScopus desc;

    SET @article_count := (SELECT count(*) from analysis_temp_hindex);
    SET @max_times_cited := (SELECT max(citation_count) from analysis_temp_hindex);
    SET @temp_hindex := (select least(@article_count, @max_times_cited)) + 1;   

    REPEAT SET @temp_hindex = @temp_hindex - 1; 
      UNTIL 
        (@temp_hindex <= (select count(*) from analysis_temp_hindex WHERE citation_count >= @temp_hindex)) OR 
        (@temp_hindex is null) OR
        (@temp_hindex = 0)
    END REPEAT;
    
    UPDATE analysis_summary_person
    SET hindexStatus = 1, hindexScopus = @temp_hindex
    WHERE personIdentifier = @person_identifier;

    SET @person_identifier = (select personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1);
                        
    UNTIL ((select count(*) from analysis_summary_person WHERE hindexStatus = 0) = 0)  
         
  END REPEAT proc1;
  
  


## 6d. Compute h5-index using Scopus data 

update analysis_summary_person h  
set hindexStatus = 0;

update analysis_summary_person h  
set hindexStatus = 1
where personIdentifier not in 
(select distinct p.personIdentifier 
from analysis_summary_person p 
join analysis_summary_author a on p.personIdentifier = a.personIdentifier
join analysis_summary_article a1 on a1.pmid = a.pmid
where citationCountScopus > 0 
and datePublicationAddedToEntrez > CURDATE() - INTERVAL 5 YEAR);  

proc1: REPEAT   

    select personIdentifier into @personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1;
                
    TRUNCATE analysis_temp_hindex;
            
    INSERT INTO analysis_temp_hindex (personIdentifier, citation_count) 
    SELECT s.personIdentifier, citationCountScopus 
    from analysis_summary_author a 
    join analysis_summary_person s on s.personIdentifier = a.personIdentifier
    join analysis_summary_article a1 on a1.pmid = a.pmid
    where s.personIdentifier = @person_identifier and citationCountScopus > 0 
    and datePublicationAddedToEntrez > CURDATE() - INTERVAL 5 YEAR 
    ORDER BY citationCountScopus desc;

    SET @article_count := (SELECT count(*) from analysis_temp_hindex);
    SET @max_times_cited := (SELECT max(citation_count) from analysis_temp_hindex);
    SET @temp_hindex := (select least(@article_count, @max_times_cited)) + 1;   

    REPEAT SET @temp_hindex = @temp_hindex - 1; 
      UNTIL 
        (@temp_hindex <= (select count(*) from analysis_temp_hindex WHERE citation_count >= @temp_hindex)) OR 
        (@temp_hindex is null) OR
        (@temp_hindex = 0)
    END REPEAT;
    
    UPDATE analysis_summary_person
    SET hindexStatus = 1, h5indexScopus = @temp_hindex
    WHERE personIdentifier = @person_identifier;

    SET @person_identifier = (select personIdentifier from analysis_summary_person WHERE hindexStatus = 0 limit 1);
                        
    UNTIL ((select count(*) from analysis_summary_person WHERE hindexStatus = 0) = 0)  
         
  END REPEAT proc1;

END IF;
END;
//
DELIMITER ;




DELIMITER //
CREATE DEFINER=`admin`@`%` PROCEDURE `updateCurateSelfRole`()
BEGIN

-- Update person info for "curate self" users from the "person" table

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.nameFirst = p.firstName
where (a.nameFirst is null or a.nameFirst = '') and p.firstName is not null;

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.nameLast = p.lastName
where (a.nameLast is null or a.nameLast = '') and p.lastName is not null;

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.nameMiddle = p.middleName
where (a.nameMiddle is null or a.nameMiddle = '') and p.middleName is not null;

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.email = p.primaryEmail
where (a.email is null or a.email = '') and p.primaryEmail is not null;

-- Add new users from person table

insert into admin_users (personIdentifier, nameFirst, nameMiddle, nameLast, email)
select personIdentifier, firstName, middleName, lastName, primaryEmail
from person p 
where p.primaryEmail not in (select email from admin_users where email is not null and email != '')
and personIdentifier not in (select personIdentifier from admin_users);

-- Create roles for them

insert into admin_users_roles (userID, roleID)
select a.userID, 4 
from person p 
join admin_users a on a.personIdentifier = p.personIdentifier
where a.userID  not in 
(select userID
from admin_users_roles
where roleID = 4);

-- Let's give self-curators the ability to generate reports for everyone

insert into admin_users_roles (userID, roleID)
select a.userID, 3 
from person p 
join admin_users a on a.personIdentifier = p.personIdentifier
left join (select userID
from admin_users_roles
where roleID = 3) x on x.userID = a.userID
where x.userID is null;


END;
//
DELIMITER ;

DELIMITER ////

CREATE DEFINER=`admin`@`%` PROCEDURE `generateEmailNotifications`(
	IN personIdentifierArray mediumblob,
    IN recipientEmail mediumblob)
BEGIN
	
    DECLARE l_person_identifier VARCHAR(250);
    DECLARE l_frequency INT;
    DECLARE l_minimum_threshold INT;
    DECLARE l_accepted INT;
    DECLARE l_suggested INT;
    DECLARE l_userID INT;
    DECLARE l_email_body TEXT;
    DECLARE l_accepted_publications_details TEXT DEFAULT '';
    DECLARE l_pending_publications_details TEXT DEFAULT '';
    DECLARE l_accepted_publications_count INT;
    DECLARE l_pending_publications_count INT;
    DECLARE l_accepted_publication_subject VARCHAR(250);
    DECLARE l_pending_publication_subject VARCHAR(250);
    DECLARE l_final_email_subject VARCHAR(250);
    DECLARE l_admin_email VARCHAR(250);
    DECLARE l_name_first VARCHAR(250);
    DECLARE l_person_article_total_score INT;
    DECLARE l_acceptedPublicationHeadLine TEXT DEFAULT '';
    DECLARE l_pendingPublicationHeadLine TEXT DEFAULT '';
    DECLARE l_accepted_pmids TEXT;
    DECLARE l_pending_pmids TEXT;
    DECLARE l_max_accepted_notifications INT;
    DECLARE l_max_suggested_notifications INT;
    DECLARE l_signature VARCHAR(2000);
    DECLARE l_view_attributes JSON;
    DECLARE l_email_sender VARCHAR(200);
    DECLARE l_email_recipient VARCHAR(200);
    DECLARE l_salutation VARCHAR(200);
    DECLARE l_accepted_pmids_det longtext;
    DECLARE l_suggested_pmids_det longtext;
    DECLARE l_admin_user varchar(250);
    DECLARE l_person_identifier_position INT;
    DECLARE l_notification_count INT;
    DECLARE l_max_message_ID INT;
    DECLARE l_notification_pref_cursor_with_cwid_cnt INT;
   
   DECLARE notification_pref_cursor CURSOR FOR SELECT DISTINCT PersonIdentifier,frequency,userID,minimumThreshold,accepted,suggested  FROM admin_notification_preferences WHERE status=1;
   DECLARE notification_pref_cursor_with_cwid CURSOR FOR SELECT DISTINCT PersonIdentifier,frequency,userID,minimumThreshold,accepted,suggested  FROM admin_notification_preferences WHERE status=1
    AND FIND_IN_SET(personIdentifier, personIdentifierArray);
   DECLARE person_cursor CURSOR FOR SELECT DISTINCT personIdentifier FROM person WHERE FIND_IN_SET(personIdentifier, personIdentifierArray);  
   
  
		DROP TEMPORARY TABLE IF EXISTS email_notifications_temp;
		
		CREATE TEMPORARY TABLE email_notifications_temp(sender varchar(250),recipient varchar(250),subject text, salutation text, accepted_subject_headline text, accepted_publications longtext,accepted_pub_count INT,suggested_subject_headline text,suggested_publications longtext,suggested_pub_count INT,
			signature text,max_accepted_publication_to_display INT,max_suggested_publication_to_display INT,personIdentifier varchar(250),accepted_publication_det longtext,suggested_publication_det longtext,admin_user_id varchar(250),notif_error_message varchar(250),pub_error_message varchar(250),max_message_id INT);
		truncate email_notifications_temp;
	
		SET SESSION group_concat_max_len = (4294967295);
	
	-- Email Sender
	   SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[1]') FROM admin_settings as2  where viewName ='EmailNotifications');
	   IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) IS NOT NULL OR JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) !='' THEN
	   		SET l_email_sender = JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView'));
	   ELSE
	   	    SET l_email_sender = 'publications@med.cornell.edu';
	   END IF;
	 
	
	  -- Email Recipient
	  	SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[8]') FROM admin_settings as2  where viewName ='EmailNotifications');
	    
	  IF recipientEmail IS NOT NULL AND recipientEmail !='' THEN 
			 SET l_email_recipient = recipientEmail;	    
	    ELSEIF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.emailOverride')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.emailOverride')) !='' 
	          AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.useEmailForScheduledJobs')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.useEmailForScheduledJobs')) = 'true' THEN
	          SET l_email_recipient = JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.emailOverride'));
	    END IF;
	 	
	  
	 -- Max limit of accepted notifications 
	  SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[5]') FROM admin_settings as2  where viewName ='EmailNotifications');
	 
	  IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.maxLimit')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.maxLimit'))!='' THEN
	  	SET l_max_accepted_notifications = JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.maxLimit'));
	  ELSE
	  	SET l_max_accepted_notifications = 5;
	  END IF;	
	 
	 -- Max limit of suggested notifications
	  SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[6]') FROM admin_settings as2  where viewName ='EmailNotifications');
	 IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.maxLimit')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.maxLimit')) !='' THEN
	 		SET l_max_suggested_notifications = JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.maxLimit'));
	 ELSE
	 		SET l_max_suggested_notifications =5;
	 END IF;
	
	-- Signiture info
	  SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[7]') FROM admin_settings as2  where viewName ='EmailNotifications');
	 IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) !='' THEN
	 		SET l_signature = JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView'));
	 ELSE
	 		SET l_signature ='Sincerely,</n>Samuel J. Wood Library</n> Weill Cornell Medicine';
	 END IF;
	
	 --  l_salutation info
	  SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[8]') FROM admin_settings as2  where viewName ='EmailNotifications');
	 IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) !='' THEN
	 		SET l_salutation = JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView'));
	 ELSE
	 		SET l_salutation ='Hi';
	 END IF;
	
	
	    OPEN person_cursor;
		BEGIN
			  DECLARE no_more_rows boolean default FALSE;
			  DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_more_rows = TRUE;	
	    person_loop : loop
	    	
		    FETCH person_cursor INTO l_person_Identifier;
		    SELECT count(*) INTO l_notification_count FROM admin_notification_preferences anp where personIdentifier = l_person_Identifier;
		    
		   IF no_more_rows THEN               
               leave person_loop;                    
            END IF; 
		   IF l_notification_count IS NOT NULL AND l_notification_count= 0 THEN  
				
		    	 INSERT INTO email_notifications_temp(
			 									  personIdentifier,notif_error_message
			 									  )
			 							VALUES(l_person_Identifier,"No notifications configured");  
		    END IF;

		end loop person_loop;
	    END;
	    CLOSE person_cursor;
		
	   OPEN notification_pref_cursor;
	    OPEN notification_pref_cursor_with_cwid;
	
	   BEGIN
		   	  DECLARE no_more_rows boolean default FALSE;
			  DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_more_rows = TRUE;	
		notification_pref_loop: loop
			
			
			IF personIdentifierArray IS NOT NULL AND personIdentifierArray!='' AND recipientEmail IS NOT NULL AND recipientEmail !='' THEN
				FETCH notification_pref_cursor_with_cwid INTO l_person_Identifier,l_frequency,l_userID,l_minimum_threshold,l_accepted,l_suggested;
			ELSE
				FETCH notification_pref_cursor INTO l_person_Identifier,l_frequency,l_userID,l_minimum_threshold,l_accepted,l_suggested;
			END IF;
		 	IF no_more_rows THEN               
               leave notification_pref_loop;                    
            END IF;

		    SET l_acceptedPublicationHeadLine ='';
		    SET l_pendingPublicationHeadLine ='';
		    SET l_accepted_publications_details = '';
    		SET l_pending_publications_details ='';

		 -- ACCEPTED PUBLICATION COUNT
    	 IF l_accepted IS NOT NULL AND l_accepted =1 THEN

    	 select count(*)  INTO l_accepted_publications_count from (
					select afl_personIdentifier as personIdentifier, x.pmid as pmid, feedback, afl_createTimestamp as createTimestamp,userID  from
					(
					select afl.personIdentifier as afl_personIdentifier, u.personIdentifier as u_personIdentifier, articleIdentifier as pmid, feedback, afl.createTimestamp as afl_createTimestamp, u.userID
					FROM admin_feedback_log afl
					left join admin_users u on u.userID = afl.userID
					where afl.personIdentifier = l_person_identifier
					order by afl.createTimestamp desc
					) x
					where x.pmid not in(select pmid from  admin_notification_log anl where anl.userID = userID and anl.notificationType ='ACCEPTED')
					and feedback = 'ACCEPTED'
					and afl_personIdentifier != u_personIdentifier
					and x.pmid is not null
					and afl_createTimestamp > DATE_SUB(CURRENT_DATE() , INTERVAL l_frequency DAY)
					group by x.pmid
					order by afl_createTimestamp desc)x;
		
	       IF l_accepted_publications_count > 1 THEN 
	          SET l_accepted_publication_subject = concat('You have ', l_accepted_publications_count,' newly accepted publications');
	       ELSEIF l_accepted_publications_count = 1 THEN
	       	  SET l_accepted_publication_subject = concat('You have ', l_accepted_publications_count ,' newly accepted publication'); 
	       ELSE
	       	   SET l_accepted_publication_subject='';	
	       END IF; 
	   END IF;   									 

	     -- pending publications count
	   IF l_suggested IS NOT NULL AND l_suggested =1 THEN
	   
	   	 SELECT count(distinct pmid)  INTO l_pending_publications_count  
  			FROM person_article pa WHERE pa.userAssertion !='ACCEPTED' AND pa.userAssertion !='REJECTED'
  											AND pa.datePublicationAddedToEntrez > DATE_SUB(CURRENT_DATE() , INTERVAL l_frequency DAY)
  											AND pa.pmid not in(select afl.articleIdentifier  from admin_feedback_log afl 
  											WHERE afl.personIdentifier =l_person_Identifier )
  											AND pa.pmid NOT IN (SELECT anl.pmid  FROM admin_notification_log anl) 
  										    AND pa.personIdentifier  = l_person_Identifier 
  										    AND pa.totalArticleScoreStandardized >= l_minimum_threshold
  										    ORDER BY pa.datePublicationAddedToEntrez DESC;
			
		   IF l_pending_publications_count > 1 THEN 
		      SET l_pending_publication_subject = concat('You have ', l_pending_publications_count,'',' pending publications for review');
	        ELSEIF l_pending_publications_count = 1 THEN  
	        	SET l_pending_publication_subject = concat('You have ', l_pending_publications_count,'',' pending publication for review'); 
	        ELSE
	            SET l_pending_publication_subject ='';
	        END IF;
  		 END IF;
  		
	  	   IF  (l_accepted_publications_count > 1 AND l_accepted=1) AND  (l_pending_publications_count > 1 AND l_suggested= 1) THEN 
	  	   		SET l_final_email_subject = CONCAT(l_pending_publication_subject ,' and ', l_accepted_publications_count, ' newly accepted publications');
	  	   	ELSEIF (l_accepted_publications_count > 1 AND l_accepted=1) AND  (l_pending_publications_count = 1 AND l_suggested= 1) THEN 
	  	   		SET l_final_email_subject = CONCAT(l_pending_publication_subject ,' and ', l_accepted_publications_count, ' newly accepted publications');
	  	   	ELSEIF (l_accepted_publications_count = 1 AND l_accepted=1) AND  (l_pending_publications_count > 1 AND l_suggested= 1) THEN 
	  	   		SET l_final_email_subject = CONCAT(l_pending_publication_subject ,' and ', l_accepted_publications_count, ' newly accepted publication');
	  	    ELSEIF  (l_accepted_publications_count =1 AND l_accepted=1) AND  (l_pending_publications_count = 1 AND l_suggested =1 )THEN 
	  	   		SET l_final_email_subject = CONCAT(l_pending_publication_subject ,' and ', l_accepted_publications_count, ' newly accepted publication');
	  	   	ELSEIF  l_accepted_publications_count > 0 AND l_accepted =1 THEN
	  	   		SET l_final_email_subject =  l_accepted_publication_subject;
	  	   ELSEIF l_pending_publications_count > 0 AND l_suggested =1 THEN
	  	   		SET l_final_email_subject =  l_pending_publication_subject;
	  	   ELSE
	  	       SET l_final_email_subject ='';
	  	   END IF;
	  	   
      
	  	   IF  l_accepted_publications_count > 0 AND l_accepted =1 THEN
	  	   
	  	   select COALESCE(group_concat(distinct x1.pmid),'') pmid INTO l_accepted_pmids from (
							select afl_personIdentifier as personIdentifier, x.pmid as pmid, feedback, afl_createTimestamp as createTimestamp,userID  from
							(
								select afl.personIdentifier as afl_personIdentifier, u.personIdentifier as u_personIdentifier, articleIdentifier as pmid, feedback, afl.createTimestamp as afl_createTimestamp, u.userID
								FROM admin_feedback_log afl
								left join admin_users u on u.userID = afl.userID
								where afl.personIdentifier = l_person_Identifier
								order by afl.createTimestamp desc
							) x
							where x.pmid not in(select pmid from  admin_notification_log anl where anl.userID = userID and anl.notificationType ='ACCEPTED')
							and feedback = 'ACCEPTED'
							and afl_personIdentifier != u_personIdentifier
							and x.pmid is not null
							and afl_createTimestamp > DATE_SUB(CURRENT_DATE() , INTERVAL l_frequency DAY)
							group by x.pmid
							order by afl_createTimestamp desc)x1;

			IF l_accepted_pmids IS NOT NULL AND l_accepted_pmids !='' AND length(l_accepted_pmids) > 0 THEN	

							-- ACCEPTED PUBLICATIONS AND TOTAL EVIDENCE SCORE
							 SELECT JSON_ARRAYAGG(DISTINCT JSON_OBJECT('PMID',afl.articleIdentifier,'totalArticleScoreStandardized',pa.totalArticleScoreStandardized))
							   INTO l_accepted_pmids_det
							   FROM admin_feedback_log afl,person_article pa 
							   WHERE FIND_IN_SET(afl.articleIdentifier , l_accepted_pmids) 
							   AND afl.personIdentifier = pa.personIdentifier
							   AND afl.articleIdentifier = pa.pmid 
							  AND afl.personIdentifier = l_person_Identifier
							  AND afl.createTimestamp  > DATE_SUB(CURRENT_DATE() , INTERVAL l_frequency DAY)
							  ORDER BY afl.createTimestamp  DESC;	
			
							SELECT COALESCE(GROUP_CONCAT(q.authorLastName,
														' ',
														LEFT(q.authorFirstName,1),' ',
														CASE
														  WHEN  q1.RANK = 2 THEN ' et al. ' 
														ELSE ''
														END,
														a.articleTitle,
														' ',
														journalTitleVerbose,
														'. ',
														a.publicationDateDisplay,
													'. PMID: ',
													a.pmid,
													'.' SEPARATOR '~!'),'')
													 INTO  l_accepted_publications_details
													FROM analysis_summary_article a
													JOIN analysis_summary_author_list q ON q.pmid = a.pmid  AND  FIND_IN_SET(q.pmid, l_accepted_pmids)
													LEFT JOIN analysis_summary_author_list q1 ON q1.pmid = a.pmid
													 AND FIND_IN_SET(q1.pmid, l_accepted_pmids)  
													WHERE q.RANK = 1
													AND (q1.RANK = 2 or q1.RANK IS NULL);	
												
				END IF;
				
				-- Accepted Publications Headerline
				  SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[3]') FROM admin_settings as2  where viewName ='EmailNotifications');
				IF l_accepted_publications_details IS NOT NULL AND l_accepted_publications_details !=''  THEN
					 IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) !='' THEN
					 		SET l_acceptedPublicationHeadLine = concat(JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')));
					 ELSE
					 		SET l_acceptedPublicationHeadLine ='The following publications have been accepted on your behalf';
					 END IF;
					 IF l_accepted_publications_count > l_max_accepted_notifications THEN
					 		SET l_acceptedPublicationHeadLine  = concat(l_acceptedPublicationHeadLine,' ','(showing the first',' ', l_max_accepted_notifications,'):');
					 ELSE
					 		SET l_acceptedPublicationHeadLine  = concat(l_acceptedPublicationHeadLine,':');
					 END IF;
			    END IF;
		  END IF;
		
		
		 IF l_pending_publications_count > 0 AND l_suggested =1 THEN
		
		 
		 SELECT COALESCE(group_concat(distinct pa.pmid),'') pmid  INTO l_pending_pmids
		   						   FROM person_article pa
		   						   WHERE pa.userAssertion !='ACCEPTED' AND pa.userAssertion !='REJECTED'
  								   AND pa.datePublicationAddedToEntrez > DATE_SUB(CURRENT_DATE() , INTERVAL l_frequency DAY)
  								   AND pa.pmid not in(SELECT afl.articleIdentifier  
  								   					  FROM admin_feedback_log afl 
  								   					  WHERE afl.personIdentifier  = l_person_Identifier)
  								   AND pa.personIdentifier  = l_person_Identifier
  								   AND pa.totalArticleScoreStandardized >= l_minimum_threshold
  								   ORDER BY pa.datePublicationAddedToEntrez DESC;
		 					
		 		IF l_pending_pmids IS NOT NULL AND l_pending_pmids!='' AND length(l_pending_pmids) > 0 THEN
		  					
		  					-- suggested PUBLICATIONS AND TOTAL EVIDENCE SCORE
				  		  	SELECT JSON_ARRAYAGG(DISTINCT JSON_OBJECT('PMID',pa.pmid,'totalArticleScoreStandardized',pa.totalArticleScoreStandardized))
							   INTO l_suggested_pmids_det
							   FROM person_article pa 
							   WHERE FIND_IN_SET(pa.pmid, l_pending_pmids) 
							   AND pa.totalArticleScoreStandardized >= l_minimum_threshold
							   AND pa.personIdentifier = l_person_Identifier
							   AND pa.datePublicationAddedToEntrez > DATE_SUB(CURRENT_DATE() , INTERVAL l_frequency DAY);	
		  		
							SELECT COALESCE(GROUP_CONCAT(DISTINCT firstAuthor,
									CASE WHEN maxRank = 1 THEN '. '
										ELSE 'et al. '
									END,
									articleTitle,' ',
									journalTitleVerbose,'. ',
									publicationDateDisplay,'. PMID: ',x.pmid,'.' SEPARATOR '~!'),'') AS z
									INTO l_pending_publications_details
							FROM person_article_author a
							JOIN person_article x ON a.pmid = x.pmid
							JOIN (SELECT DISTINCT pmid, max(rank) AS maxRank FROM person_article_author WHERE FIND_IN_SET(pmid, l_pending_pmids) GROUP BY pmid) y ON y.pmid = a.pmid 
							JOIN (SELECT DISTINCT pmid, CONCAT(authorLastName, ' ', LEFT(authorFirstName, 1), ' ') AS firstAuthor FROM person_article_author WHERE rank = 1 AND FIND_IN_SET(pmid, l_pending_pmids)) m ON m.pmid = a.pmid
							WHERE FIND_IN_SET(a.pmid, l_pending_pmids); 	
						
				END IF;	
				-- Suggested Publications Headerline
				  SET l_view_attributes = (SELECT JSON_EXTRACT(viewAttributes,'$[4]') FROM admin_settings as2  where viewName ='EmailNotifications');
			    
				 IF l_pending_publications_details IS NOT NULL AND l_pending_publications_details !='' THEN
			    	
			    	IF JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')) !='' THEN
					 	 
			    		SET l_pendingPublicationHeadLine = concat(JSON_UNQUOTE(JSON_EXTRACT(l_view_attributes,'$.labelUserView')));
					 ELSE
					 		SET l_pendingPublicationHeadLine = 'The following publications are pending for you';
					 END IF;
					 IF l_pending_publications_count > l_max_suggested_notifications THEN
					 		SET l_pendingPublicationHeadLine  = concat(l_pendingPublicationHeadLine,' ','(showing the first',' ', l_max_suggested_notifications,'):');
					 ELSE
					 		SET l_pendingPublicationHeadLine  = concat(l_pendingPublicationHeadLine,':');
					 END IF;
				END IF;
		
		END IF;	
	
		IF  (l_accepted_publications_count > 0 AND l_accepted =1 
	   			 AND l_accepted_pmids IS NOT NULL AND l_accepted_pmids !='' AND length(l_accepted_pmids) > 0 AND l_accepted_pmids_det IS NOT NULL AND l_accepted_pmids_det !='' AND length(l_accepted_pmids_det) > 0 
	   					  && l_accepted_publications_details IS NOT NULL AND l_accepted_publications_details!='' AND length(l_accepted_publications_details) > 0) 
	  		 OR  (l_pending_publications_count > 0 AND l_suggested =1 
	  		   AND l_pending_pmids IS NOT NULL AND l_pending_pmids!='' AND length(l_pending_pmids) > 0 AND l_suggested_pmids_det IS NOT NULL AND l_suggested_pmids_det !='' AND length(l_suggested_pmids_det)> 0
	  		 			 && l_pending_publications_details IS NOT NULL AND l_pending_publications_details !='' AND length(l_pending_publications_details) > 0
	  		 			) THEN 
			
			     -- Admin Email
			  SELECT  email, au.nameFirst,au.userID  INTO l_admin_email,l_name_first,l_admin_user 
			  FROM admin_users au WHERE au.personIdentifier = l_person_Identifier; 
			 
			 SELECT COALESCE(MAX(messageID), 0) INTO l_max_message_ID  
			 FROM admin_notification_log anl;
			
			 INSERT INTO email_notifications_temp(sender,
			 									  recipient,
			 									  subject, 
			 									  salutation, 
			 									  accepted_subject_headline,
			 									  accepted_publications,
			 									  accepted_pub_count,
			 									  suggested_subject_headline,
			 									  suggested_publications,
			 									  suggested_pub_count,
			 									  signature,
			 									  max_accepted_publication_to_display,
			 									  max_suggested_publication_to_display,
			 									  personIdentifier,
			 									  accepted_publication_det,
			 									  suggested_publication_det,
			 									  admin_user_id,
			 									  max_message_id)
			 							VALUES(l_email_sender,
			 									COALESCE(l_email_recipient,l_admin_email),
			 									l_final_email_subject,
			 									concat(l_salutation,' ',l_name_first),
			 									l_acceptedPublicationHeadLine,
			 									l_accepted_publications_details,
			 									l_accepted_publications_count,
			 									l_pendingPublicationHeadLine,
			 									l_pending_publications_details,
			 									l_pending_publications_count,
			 									l_signature,
			 									l_max_accepted_notifications,
			 									l_max_suggested_notifications,
			 								    l_person_Identifier,
			 								    l_accepted_pmids_det,
    										    l_suggested_pmids_det,
    										    l_admin_user,
    										   l_max_message_ID);
    										   
    	ELSEIF l_notification_pref_cursor_with_cwid_cnt IS NOT NULL AND l_notification_pref_cursor_with_cwid_cnt > 0 THEN  -- INSERT NO ELIGIGBLE PUBLICATIONS FOUND MESSAGE
    		
    		INSERT INTO email_notifications_temp(
			 									  personIdentifier,pub_error_message
			 									  )
			 							VALUES(l_person_Identifier,"No eligible publications");
    		
    	  
		END IF;
	
	
			-- UNTIL done END REPEAT;
		END LOOP notification_pref_loop;
	    END;
	    CLOSE notification_pref_cursor_with_cwid;
		CLOSE notification_pref_cursor;
	    
		SELECT * FROM email_notifications_temp;
	
END
//
DELIMITER ;


CREATE DEFINER=`admin`@`%` EVENT `runPopulateAnalysisSummaryPersonScopeTable` ON SCHEDULE EVERY 1 DAY STARTS '2022-01-01 01:00:00' ON COMPLETION PRESERVE ENABLE DO call `populateAnalysisSummaryPersonScopeTable`();

CREATE DEFINER=`admin`@`%` EVENT `runPopulateAnalysisSummaryTables` ON SCHEDULE EVERY 1 DAY STARTS '2022-01-01 01:00:00' ON COMPLETION PRESERVE ENABLE DO call `populateAnalysisSummaryTables`();

CREATE DEFINER=`admin`@`%` EVENT `runUpdateCurateSelfRole` ON SCHEDULE EVERY 1 DAY STARTS '2022-01-01 01:00:00' ON COMPLETION PRESERVE ENABLE DO call `updateCurateSelfRole`();
