<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Generate edge case CSV data with missing values and malformed dates

Here are edge‑case CSV snippets you can save as separate files (for example `c01_edge.csv`, `co2_edge.csv`, `co3_edge.csv`).

***

## c01_edge.csv (contact / target / person\_a / person\_b)

```csv
contact,target,person_a,person_b,date_of_first_interaction,date_of_last_interaction,additional_info,dataset_name
, , , ,"","",,c01
"   ","None","   Alice  ","  Bob   ","2024-13-01","2024-02-30","Invalid months and days",c01
+86 1380013800X,13900ABC001,Chen Wei,Liu Fang,2024/02/30,2024/02/29,"Non-numeric in phone, impossible dates",c01
NULL,null,NULL,null,20240230,2024.02.29,"Upper/lower null, yyyymmdd and dotted date",c01
""," ",Zhang Yan,Gao Peng,2024-02-29,29/02/2023,"Leap-year edge and dd/mm/yyyy",c01
+86-00000000000,+86 123, , ,not_a_date,2024-00-10,"Clearly malformed date and phone",c01
" 15012345678 ","1501234567890", "  ","  ",2024/2/5,5/2/2024,"Single-digit month/day in different orders",c01
"None","",Alice,,none,Null,"Text none/Null in date",c01
```


***

## co2_edge.csv (selector\_a / selector\_b / name\_a / name\_b)

```csv
selector_a,selector_b,name_a,name_b,date_of_interaction,additional_info,comment,dataset_name
,,,"","",,,""
"   ","   ","   ","   ","   ","   ","   ",co2
+86 (010) 1234 5678,86-010-1234-5678, Li Ming , ,2024-00-10,"Zero month","Bad month in ISO format",co2
13123456789X,1A323456789,Chen Jie,Sun Lei,2024-02-30,"Bad day","Non-existent day",co2
"None","none","None","none",None,None,None,co2
" ","",Wu Gang,He Ping,32/13/2024,"Day>31, month>12","European-style but invalid",co2
+86-,,,,2024/13/01,"Missing phone digits","Slash date with bad month",co2
1701234567,1701234567890,Zhang San,Li Si,202402,,"Too short/long phone, yyyymm (ambiguous)",co2
170 1234 5678, ,  ,Ma Ba,2024.2.3,"Dotted date, minimal name","",co2
```


***

## co3_edge.csv (same layout as co2)

```csv
selector_a,selector_b,name_a,name_b,date_of_interaction,additional_info,comment,dataset_name
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
"","+86 15512345678","  ","Zheng Liu","",,"Empty date and names",co3
+86 1551234567A,+86-1562345B789,Hu Yi, Ma Ba ,2024/2/30,"Invalid day","Alpha chars in phone",co3
(029)3333-4444,028 2222 3333, Zhou Jiu , Qin Shi ,2024/13/01,"Month 13","Slash date invalid",co3
"   17012345670   "," 86 170 1234 5671 ",,,"not_a_date","Free text date","",co3
"None"," ","None",,"2024-2-3","Unpadded month/day","Text none in phones and names",co3
155-1234-5679,156 2345 6780,"","",0000-00-00,"Zeroed date","All zeros",co3
,,"   Alice","Bob   ","02/31/2024","US-style invalid date","",co3
```

These rows include:

- Empty strings, whitespace-only cells, `None`, `NULL`, and missing fields to test your “Unknown” replacement logic.
- Malformed and ambiguous dates (bad months/days, unusual formats, text in date fields).
- Non-numeric and oddly formatted phone numbers to stress-test your China phone normalization.

