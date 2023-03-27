WITH factor
AS 
(
SELECT 
    n.Base_BoID
    , u.Date
    , u.Hour
	, SUM(u.Supply_Net) 															AS Vehicles
    , LOG(2, CASE WHEN SUM(u.Supply_Net) < 1 THEN 1 ELSE SUM(u.Supply_Net) END) 
    	* (
			SELECT
				CAST(VALUE AS FLOAT) AS MINDATE
			FROM {{ source('meta_model', 'T_CONFIGURATION') }}
			WHERE UPPER(NAME) = 'TARGETSLOPE'
		) + (
			SELECT
				CAST(VALUE AS FLOAT) AS MINDATE
			FROM {{ source('meta_model', 'T_CONFIGURATION') }}
			WHERE UPPER(NAME) = 'TARGETINTERCEPT'
		)																			AS TargetFactor
FROM {{ source('consume_public', 'T_M2N_BASENEIGHBORHOOD') }} n
	INNER JOIN {{ref('t_utilization')}} u 
    	ON	n.NEARBASE_BOID = u.Base_BoID
GROUP BY n.Base_BoID
    , u.Date
    , u.Hour
)
, CTE_fact_utilization_supply
as
(
	SELECT 
		u.BASE_BOID
		, u.CARRIER_UID
		, u.UTILIZATIONID
		, u.VEHICLE_UID
		, u.MUNICIPALITY_ID								-- V4.1 added
		, u.DATE
		, u.HOUR
		, u.SUPPLY_GROSS
		, u.SUPPLY_NET
		, u.SUPPLY_NET
			* IFNULL(f.TargetFactor, (
				SELECT
					CAST(VALUE AS FLOAT) AS MINDATE
				FROM META.MODEL.T_CONFIGURATION
				WHERE UPPER(NAME) = 'TARGETINTERCEPT'
				))										-- TargetFactor from Neiborhood-Calculation, take default TargetIntercept if missing
			* IFNULL(cm.MODIFICATOR, 1)					-- Category-Modificator
			* IFNULL(mm.MODIFICATOR, 1)					-- Season-Modificator
			* IFNULL(hwm.MODIFICATOR, 1)				-- Hour-of-week Modificator
			AS DEMANDTARGET_NET
		, u.BUDGET1
		, u.BUDGET2
	FROM {{ref('t_utilization')}} u
		LEFT JOIN {{ source('meta_model', 'T_UTILIZATIONTARGETMOD_HOUROFWEEK') }} hwm
			ON	hwm.DayOfWeek = DAYOFWEEKISO(u.DATE)
			AND	hwm.Hour = u.HOUR
		LEFT JOIN {{ source('meta_model', 'T_UTILIZATIONTARGETMOD_MONTH') }} mm
			ON	mm.MONTHNO = MONTH(u.DATE)
		LEFT JOIN {{ref('t_utilization_carrier')}} c
			ON	c.CARRIER_UID = u.CARRIER_UID
		LEFT JOIN {{ source('meta_model', 'T_UTILIZATIONTARGETMOD_CATEGORY') }} cm
			ON	cm.CATEGORY_BOID = c.CATEGORY_BOID
		LEFT JOIN factor f
			ON	f.Base_BoID = u.Base_BoID
			AND	f.DATE = u.DATE
			AND f.HOUR = u.HOUR
	ORDER BY
		u.DATE
)
select * from CTE_fact_utilization_supply
