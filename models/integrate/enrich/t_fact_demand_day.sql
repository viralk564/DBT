WITH CTE_fact_demand_day
as 
(
	SELECT 
		BASE_BOID 
		, CARRIER_UID 
		, CONTRACT_BOID 
		, MAINABO_UID 
		, PERSON_BOID 
		, RESERVATION_BOID 
		, UTILIZATIONID 
		, VEHICLE_UID 
		, MUNICIPALITY_ID								-- V4.1 added
		, "DATE" 
		, SUM(DEMAND_GROSS)								as DEMAND_GROSS
		, SUM(DEMAND_NET)								as DEMAND_NET
		, SUM(DRIVE_KM)									as DRIVE_KM
		, SUM(REVENUE_NET)								as REVENUE_NET
		, SUM(RES_START)								as RES_START
		, SUM(RES_END)									as RES_END
		, SUM(DRIVE_START)								as DRIVE_START
		, SUM(DRIVE_END)								as DRIVE_END
	FROM {{ref('t_fact_utilization_demand')}}
	WHERE "DATE" < CURRENT_DATE
	GROUP BY
		BASE_BOID 
		, CARRIER_UID 
		, CONTRACT_BOID 
		, MAINABO_UID 
		, PERSON_BOID 
		, RESERVATION_BOID 
		, UTILIZATIONID 
		, VEHICLE_UID 
		, MUNICIPALITY_ID								-- V4.1 added
		, "DATE" 
)
select * from CTE_fact_demand_day