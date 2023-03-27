WITH CTE_fact_supply_day
as 
(
	SELECT 
		BASE_BOID 
		, CARRIER_UID 
		, UTILIZATIONID 
		, VEHICLE_UID 
		, MUNICIPALITY_ID								-- V4.1 added
		, "DATE"  
		, SUM(Supply_Gross)								as Supply_Gross
		, SUM(Supply_Net)								as Supply_Net
		, SUM(DemandTarget_Net)							as DemandTarget_Net
		, SUM(Budget1)									as Budget1
		, SUM(Budget2)									as Budget2
	FROM {{ref('t_fact_utilization_supply')}}
	WHERE "DATE" < CURRENT_DATE
	GROUP BY
		BASE_BOID 
		, CARRIER_UID 
		, UTILIZATIONID 
		, VEHICLE_UID 
		, MUNICIPALITY_ID								-- V4.1 added
		, "DATE" 
		
	UNION ALL	
	
	SELECT 
		  NULL 			AS BASE_BOID 
		, NULL 			AS CARRIER_UID 
		, NULL 			AS UTILIZATIONID 
		, NULL 			AS VEHICLE_UID 
		, NULL 			AS MUNICIPALITY_ID								-- V4.1 added 
		, "DATE"  		
		, NULL			as Supply_Gross
		, NULL			as Supply_Net
		, NULL			as DemandTarget_Net
		, NULL			as Budget1
		, NULL			as Budget2
	FROM {{ source('consume_public', 'T_DIM_DATE') }}
	WHERE DATE >= (
			SELECT
				BIZBEG
			FROM {{ref('t_utilization_interval')}}
		)
		AND DATE < CURRENT_DATE
)
select * from CTE_fact_supply_day