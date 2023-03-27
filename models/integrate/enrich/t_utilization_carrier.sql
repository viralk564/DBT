with CTE_utilization_carrier
as 
(
	SELECT
		-- fields used for time-slicing
		k.CARRIER_UID 
		, GREATEST(j.BIZBEG, m.BizBeg) AS BizBeg
		, LEAST(j.BIZEND, m.BizEnd) AS BizEnd
		-- fields used after time-slicing
		, k.CARRIER_BOID
		, j.BASE_BOID 
		, j.VEHICLE_BOID 
		, b.MUNICIPALITY_ID			-- V4.1 added
		, j.BUDGET1 
		, j.BUDGET2 
		, j.CATEGORY_BOID 
		
	FROM {{ref('t_utilization_interval')}} m
		INNER JOIN {{ source('integrate_join', 'V_CARRIER') }} j
			ON m.BizBeg < j.BIZEND
			AND m.BizEnd > j.BIZBEG 
			AND j.BIZBEG < j.BIZEND
		INNER JOIN {{ source('integrate_key', 'T_CARRIER') }} k
			ON	k.CARRIER_BOID = j.CARRIER_BOID
			AND	k.BIZBEG = j.BIZBEG 
			AND	k.BIZEND = j.BIZEND 
		INNER JOIN {{ source('integrate_join', 'V_BASE') }} b		-- V4.1 added
			ON j.BASE_BOID = b.BASE_BOID 
	WHERE j.STATE_CTID = 1
		AND j.BASE_BOID > 0
)

select * from CTE_utilization_carrier
