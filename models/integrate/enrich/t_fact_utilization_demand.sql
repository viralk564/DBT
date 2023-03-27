WITH CTE_utilization_demand
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
		, DATE
		, HOUR
		, DEMAND_GROSS
		, DEMAND_NET
		, DRIVE_KM
		, REVENUE_NET
		, RES_START
		, RES_END
		, DRIVE_START
		, DRIVE_END
		
	FROM {{ref('t_utilization')}}
	ORDER BY
		DATE
)
select * from CTE_utilization_demand