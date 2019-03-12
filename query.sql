/*Percent of sales of every manager to all sales by region*/
/*
with allsales as (select  regions.name as region
, managers.name as manager
,sum(salesumm) over(PARTITION BY  managers.name) as managertotal
, sum(salesumm) over(PARTITION BY regions.name) as regiontotal
,round((sum(salesumm) over(PARTITION BY  managers.name)/sum(salesumm) over(PARTITION BY regions.name)),3)*100 as percent 
from regions left join managers
on regions.id = managers.regionid
left join sales
on managers.id = sales.managerid
) 
select distinct * from allsales
order by region, manager
*/

/*Everyday Sales of managers during one month for sum of each day*/
/*
with allsalesbydate as (select 
managers.name as manager, sales.saledate as saledate, EXTRACT(day FROM sales.saledate) as mday
,row_number() over(order BY   EXTRACT(day FROM sales.saledate) ) as rownumber
,dense_rank() over(order BY   EXTRACT(day FROM sales.saledate) ) as danserank
, sum(salesumm) over(PARTITION BY  EXTRACT(day FROM sales.saledate) order BY  EXTRACT(day FROM sales.saledate)) as datemanagertotal
from managers left join sales
on managers.id = sales.managerid
where sales.saledate between '01.03.2019' and '31.03.2019'
) 
select distinct danserank,  mday, manager, datemanagertotal from allsalesbydate
order by danserank
*/

/*Everyday Sales of managers during one month for sum of each day with commulative result*/
with allsalesbydate as (select 
managers.name as manager, sales.saledate as saledate, EXTRACT(day FROM sales.saledate) as mday
,dense_rank() over(order BY  managers.name ) as danserank
, sum(salesumm) over(PARTITION BY  managers.name, EXTRACT(day FROM sales.saledate) order BY  EXTRACT(day FROM sales.saledate) ) as datemanagertotal
from managers left join sales
on managers.id = sales.managerid
where sales.saledate between '01.03.2019' and '31.03.2019'
) 

select mday, manager,datemanagertotal
,sum(datemanagertotal) over(partition BY   danserank order by mday) as managercomulativetotal
from (select  distinct  mday,  manager, datemanagertotal,  danserank from allsalesbydate)
order by mday, manager



