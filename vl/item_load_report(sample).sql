/* 
 * Item Load Validations - Report.sql
 * Placeholders: {py_table_name}, {py_linked_server}
 */

/************** Data Checks ****************/
--INFO - Count Total Rows
select count(*)cnt 
from {py_table_name}
;

--INFO - Count Total Items
select count(distinct tc_id)
from {py_table_name}
;

--INFO - Data Exctract 200
select * 
from {py_table_name}
where rownum <= 200
;

--INFO - Item Staged Twice
select * from (
   select chk.*, count(*) over (partition by upper(tc_id)
      ,upper(revision)) as cnt
   from {py_table_name} chk
   )
where cnt >1
order by tc_id
;

/************** Team Center Checks ****************/ 
--ISSUE - Item Exist in DW
select /*+DRIVING_SITE(dw)+*/
    chk.tc_id, dw.* 
from {py_table_name}  chk
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
;

--ISSUE - Item Exist in TC
select /*+DRIVING_SITE(item)+*/
    count(*) as count
from {py_table_name}  chk
join infodba.pitem@{py_linked_server} item on 
	upper(item.pitem_id) = chk.tc_id
join infodba.ppom_object@{py_linked_server} obj on
	obj.puid = item.puid and 
	obj.rowning_siteu is null
;

--ISSUE - Alt ID Exist in TC
select 
    chk.tc_id, chk.alternateid given_altid
    ,alt.pidcxt_name cxt, alt.pitem_id actual_related_item
    ,alt.item_group, alt.local_site 
from {py_table_name} chk
join altid_all_site alt on
    upper(alt.pidfr_id) = chk.alternateid 
;

--ISSUE - Item with different Alt ID in TC
select chk.tc_id, alternateid,pidfr_id current_altid
from {py_table_name} chk
join altid_all_site oid on upper(oid.pitem_id) = chk.tc_id
where chk.alternateid <> pidfr_id
;

/************** Suffix Checks ****************/
--INFO - Item ID Suffix
select nvl( suffix, 'None' ) suffix, count(*)cnt
from( select chk.*, regexp_substr( tc_id, '\:.*' ) Suffix
from {py_table_name} chk)
group by suffix
;

--INFO - Duplicate Suffix
select distinct tc_id from {py_table_name}  
where tc_id like '%:%:%'
;

--INFO - Item with no Suffix
select distinct tc_id from {py_table_name} 
where (TC_ID not like '%:%P')
;

/************** REV Checks ****************/
--ISSUE - Parts with multiple REVs
Select tc_id, count(distinct revision)rev_count
    ,listagg(revision,', ') within group (
        order by revision) revision_list
from {py_table_name}  
group by tc_id
having count(distinct revision) >1
;

--ISSUE - Revisions with Same Creation Date
select 
    tc_id, rev_creation_date, cnt_revs_same_date
    ,listagg(revision, ', ') within group (
        order by rev_creation_date, revision ) as revisions_samedate
from (
    select distinct 
        tc_id, rev_creation_date 
        ,count(distinct revision) over (
            partition by tc_id, rev_creation_date ) as cnt_revs_same_date
        ,revision 
    from {py_table_name}
) 
where cnt_revs_same_date > 1
group by tc_id, rev_creation_date, cnt_revs_same_date
order by tc_id
;

--ISSUE - REV is null
select tc_id, revision
from {py_table_name} chk 
where revision is null
;

--INFO - REV Counts
select revision, count(*)cnt
from {py_table_name} chk 
group by revision
order by 1
;

/************** TC ID Checks ****************/  
--ISSUE - TC ID Length Grater Than 30 Chars
select tc_id, length(tc_id)
from {py_table_name}  
where length(tc_id) >30
;

--ISSUE - TC ID Grater Than 32 Chars
select tc_id, length(tc_id)
from {py_table_name}  
where length(tc_id) >32
;

--INFO - Spaces in TC ID
select tc_id
from {py_table_name}
where regexp_like(Tc_Id,  '\s')
;

--INFO - TC ID with Trailing Space
select tc_id 
from {py_table_name}  
where regexp_like(Tc_Id, '\s+:.*$|\s+$|:\s+.*$')
;

--INFO - TC ID  with Special Chars
select tc_id
from {py_table_name}  
where regexp_like(tc_id,'[/\$#%%~,"`?*)(%%|^!?]')
;

/************** Alt ID Checks ****************/  
--INFO - Item with different Alt ID
select upper(tc_id)tc_id, count(distinct alternateid)cnt
from {py_table_name}
group by upper(tc_id)
having count(distinct alternateid)>1
;

--INFO - Alt ID on different Items
select upper(alternateid)alternateid, count(distinct tc_id)cnt
from {py_table_name}
where alternateid is not null
group by upper(alternateid)
having count(distinct tc_id)>1
;

--ISSUE - Alt ID Grater Than 32 Chars
select alternateid, length(alternateid)
from {py_table_name}  
where length(alternateid) >32
;

--INFO - Spaces in Alt ID
select alternateid
from {py_table_name}
where regexp_like(alternateid,  '\s')
;

--INFO - Alt ID with Trailing Space
select alternateid 
from {py_table_name}  
where regexp_like(alternateid, '\s+:.*$|\s+$|:\s+.*$')
;

--INFO - Alt ID  with Special Chars
select alternateid
from {py_table_name}  
where regexp_like(alternateid,'[/\$#%%~,"`?*)(%%|^!?]')
;

--ISSUE - Alt ID has colon
select alternateid
from {py_table_name}  
where regexp_like(alternateid,':')
;

/************** Item Name1 Checks ****************/ 
--ISSUE - Item Name1 is Null
select tc_id, name
from {py_table_name} 
where name is null
;

--INFO - Item Name1 Length Greater than 30 chars
select tc_id, name, length(name) as length
from {py_table_name}
where length(name) > 30
;

--INFO - Item Name1 Length Greater than 32 chars (Oracle)
select tc_id, name, length(name) as length
from {py_table_name}
where length(name) > 32
;

--INFO - Item Name1 Non-ASCII
select tc_id,name ,asciistr(name)
from {py_table_name}
where name <> asciistr(name)
;

/************** Item Name2 Checks ****************/ 
--INFO - Item Name2 is Null
select count(*) as count
--	tc_id, name2
from {py_table_name} 
where name2 is null
;

--INFO - Item Name2 Length Greater than 30 chars
select  tc_id, name2, length(name2) as length
from {py_table_name}
where length(name2) > 30
;

--INFO - Item Name2 Non-ASCII
select tc_id,name2 ,asciistr(name2)
from {py_table_name}
where name2 <> asciistr(name2)
;

/************** Item Desc Checks ****************/ 
--INFO - Item Desc is Null
select count(*) as count
--	tc_id, description
from {py_table_name}
where description is null
;

--INFO - Item Desc Greater than 240 chars 
select tc_id, description, length(description) as length
from {py_table_name}
where length(description) > 240
;

--INFO - Item Desc has pipe
select tc_id, description
from {py_table_name}  
where regexp_like(description,'\|')
;

--ISSUE - Item Desc has Control Chars
select tc_id, description,  regexp_replace(description,'[[:cntrl:]]',' ')
from {py_table_name}  
where (regexp_like(description,'[[:cntrl:]]') or regexp_like(name,'[[:cntrl:]]'))
;

/************** Date Checks ****************/
--INFO - Release Date is Null
select count(*)count
from {py_table_name}
where rev_release_date is null
;

--INFO - Creation Date is Null
select count(*)count
from {py_table_name}
where rev_creation_date is null
;

--INFO - Release Dates
select 
	substr(extract(year from rev_release_date), 1, 3)||'0s' as decade
	,count(*) as count
from {py_table_name}
group by substr(extract(year from rev_release_date), 1, 3)
order by 1
;

--INFO - Creation Dates
select 
	substr(extract(year from rev_creation_date), 1, 3)||'0s' as decade
	,count(*) as count
from {py_table_name}
group by substr(extract(year from rev_creation_date), 1, 3)
order by 1
;

/************** UOM Checks ****************/ 
--ISSUE - Invalid TC UOM (CAPS Stage)
select /*+DRIVING_SITE(tcuom)+*/
--    chk.tc_id, chk.uom given_uom, tcuom.psymbol match_uom
	distinct
	chk.uom given_uom
	,count(distinct chk.tc_id) over(
		partition by chk.uom) as cnt
from {py_table_name} chk 
left join
      (select 
           val.pval_0 rsuom,lovdesc.pval_0 lov_desc, uom.psymbol
           ,upper(substr(lovdesc.pval_0,0,instr(lovdesc.pval_0,',',1,1)-1))
           ,val.pval_0||'='||uom.psymbol
       from 
                infodba.plistofvalues@houtc_stby lov  
           join infodba.plov_values_2@houtc_stby val on 
               lov.puid = val.puid  
           join infodba.plov_value_descriptions_2@houtc_stby lovdesc on 
               lovdesc.puid = val.puid and val.pseq = lovdesc.pseq 
           join infodba.punitofmeasure@houtc_stby  uom on 
               upper(uom.psymbol) = upper(substr(lovdesc.pval_0,0,instr(
                   lovdesc.pval_0,',',1,1)-1) )
       where 
           lov.plov_name = '_RSOne_UnitsOfMeasure_' 
           and lovdesc.pval_0 like '%%RSONE%%'
--      )tcuom on chk.uom = tcuom.rsuom
      )tcuom on chk.uom = tcuom.psymbol
where tcuom.psymbol is null
order by chk.uom
;

--ISSUE - Invalid TC UOM (JDE Stage)
select /*+DRIVING_SITE(tcuom)+*/
--    chk.tc_id, chk.uom given_uom, tcuom.psymbol match_uom
	distinct
	chk.uom given_uom
	,count(distinct chk.tc_id) over(
		partition by chk.uom) as cnt
from {py_table_name} chk 
left join
      (select 
           val.pval_0 rsuom,lovdesc.pval_0 lov_desc, uom.psymbol
           ,upper(substr(lovdesc.pval_0,0,instr(lovdesc.pval_0,',',1,1)-1))
           ,val.pval_0||'='||uom.psymbol
       from 
                infodba.plistofvalues@houtc_stby lov  
           join infodba.plov_values_2@houtc_stby val on 
               lov.puid = val.puid  
           join infodba.plov_value_descriptions_2@houtc_stby lovdesc on 
               lovdesc.puid = val.puid and val.pseq = lovdesc.pseq 
           join infodba.punitofmeasure@houtc_stby  uom on 
               upper(uom.psymbol) = upper(substr(lovdesc.pval_0,0,instr(
                   lovdesc.pval_0,',',1,1)-1) )
       where 
           lov.plov_name = '_RSOne_UnitsOfMeasure_' 
           and lovdesc.pval_0 like '%%RSONE%%'
      )tcuom on chk.uom = tcuom.rsuom
--      )tcuom on chk.uom = tcuom.psymbol
where tcuom.psymbol is null
order by chk.uom
;

/************** Group Checks ****************/
--INFO - Owning Group
select /*+driving_site(dw)*/
	distinct
	g.primary_site
    ,g.business_unit
    ,g.parent_name
    ,chk.owninggroup grp
    ,g."ERPS"
    ,count(*) over (
    	partition by chk.owninggroup) as cnt
from {py_table_name} chk
left join cdmrpt.dim_group_master@cdmdw g on
	upper(g.group_name) = upper(chk.owninggroup)
order by cnt desc
;

/************** Status Checks ****************/
--INFO - Item Status
select item_status, count(*)count
from {py_table_name} chk
group by item_status
;

--INFO - Revision Status
select revision_status, count(*)count
from {py_table_name} chk
group by revision_status
;

/************* Existing Items (Delta Items) *************/
--INFO - Existing - Revision Updates
select /*+DRIVING_SITE(dw)+*/
	dw.tc_id ||'|'|| rev.pitem_revision_id ||'|'|| revision as inp,
	chk.tc_id
	,rev.pitem_revision_id as rev_current
	,chk.revision as rev_new
from {py_table_name}  chk
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
join infodba.pitemrevision@{py_linked_server} rev on 
	rev.ritems_tagu = dw.puid
where
	chk.revision <> rev.pitem_Revision_id
;

--INFO - Existing - Parts With Multiple Revisions
select * from (
select /*+DRIVING_SITE(dw)+*/
	chk.tc_id
	,rev.pitem_revision_id as rev_current
	,count(*) over(
		partition by chk.tc_id) cnt 
from {py_table_name}  chk
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
join infodba.pitemrevision@{py_linked_server} rev on 
	rev.ritems_tagu = dw.puid
) where 
	cnt > 1
;

--INFO - Existing - Status Updates
select /*+DRIVING_SITE(dw)+*/
	dw.tc_id ||'|'|| chk.item_status as inp,
	chk.tc_id
	,dw.status as status_current
	,chk.item_status as status_new
from {py_table_name}  chk
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
left join infodba.prelease_status_list@{py_linked_server} rsl on 
	rsl.puid = dw.puid 
left join infodba.preleasestatus@{py_linked_server} sta on 
	sta.puid = rsl.pvalu_0
where 
	nvl(upper(trim(item_status)), 'none') <> nvl(upper(sta.pname), 'none')
;

--INFO - Existing - Item Name Updates
select /*+DRIVING_SITE(dw)+*/
	dw.tc_id||'|'||chk.name ||'|IGNORE' as inp,
	chk.tc_id
	,dw.pobject_name as name_current
	,length(dw.pobject_name) as length
	,chk.name as name_new
	,length(chk.name) as length
from {py_table_name}  chk 
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
join infodba.pworkspaceobject@{py_linked_server} wbj on 
	wbj.puid = dw.puid
where 
	upper(regexp_replace(name, '\W')) <> upper(regexp_replace(wbj.pobject_name, '\W'))
;

--INFO - Existing - Item Description Updates
select /*+DRIVING_SITE(dw)+*/
	dw.tc_id||'|IGNORE|'||chk.description as inp,
	chk.tc_id
	,dw.pobject_desc as description_current
	,chk.description as description_new
from {py_table_name}  chk 
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
join infodba.pworkspaceobject@{py_linked_server} wbj on 
	wbj.puid = dw.puid
where 
	upper(regexp_replace(description, '\W')) <> upper(regexp_replace(wbj.pobject_desc, '\W'))
;

--INFO - Existing - Item Type Updates
select /*+DRIVING_SITE(dw)+*/
	chk.tc_id
	,prsone_itemtype as type_current
	,engineering_type as type_new
from {py_table_name}  chk
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
join infodba.pimanrelation@{py_linked_server} iman on 
	iman.rprimary_objectu = dw.puid
join infodba.pimantype@{py_linked_server} rel on 
	rel.puid = iman.rrelation_typeu
	and rel.ptype_name = 'IMAN_master_form'
join infodba.pform@{py_linked_server} frm on 
	frm.puid = iman.rsecondary_objectu		 
left join (
	select puid, prsone_itemtype from infodba.pnoipartmaster@{py_linked_server} union 
  	select puid, prsone_itemtype from infodba.pnoinonengineeringmaster@{py_linked_server}
) pm on pm.puid = frm.rdata_fileu
where 
	nvl(chk.engineering_type, 'none') <> nvl(prsone_itemtype, 'none')
;

--INFO - Existing - UOM Updates
select /*+DRIVING_SITE(dw)+*/
	chk.tc_id
	,uom.psymbol as uom_current
	,chk.uom as uom_new
from {py_table_name}  chk
join cdmrpt.item_all_site@cdmdw dw on 
	upper(dw.tc_id) = chk.tc_id
join infodba.pitem@{py_linked_server} item on 
	item.puid = dw.puid
join infodba.punitofmeasure@{py_linked_server} uom on 
	uom.puid = item.ruom_tagu 
where 
	nvl(chk.uom, 'none') <> nvl(uom.psymbol, 'none')
;
