with CTE_utilization_dateTime
as 
(
    SELECT
        da.DATE
        , ti.HOUR
        , dateadd(HOUR, ti.HOUR, da.DATE)		AS BizBeg
        , dateadd(HOUR, ti.HOUR + 1, da.DATE)	AS BizEnd
    FROM {{ref('t_utilization_interval')}} m
        INNER JOIN {{ source('consume_public', 'T_DIM_DATE') }} da
            ON m.BizBeg <= da."DATE" 
            AND m.BizEnd > da."DATE" 
        CROSS JOIN {{ source('consume_public', 'T_DIM_TIME') }} ti
),
CTE_utilization_vehicle
as 
(
    SELECT 
		-- fields used for time-slicing
		ca.CARRIER_UID 
		, GREATEST(ca.BIZBEG, ve.BIZBEG) AS BIZBEG
		, LEAST(ca.BIZEND, ve.BIZEND) AS BIZEND
	FROM {{ref('t_utilization_carrier')}}  ca
		INNER JOIN {{ source('integrate_join', 'V_VEHICLE') }} ve
			ON	ca.VEHICLE_BOID = ve.VEHICLE_BOID
			AND	ca.BIZBEG < ve.BIZEND 
			AND ca.BIZEND > ve.BIZBEG 
			AND ve.BIZBEG < ve.BIZEND
),
CTE_utilization_reservation
as 
(
	SELECT
		-- fields used for time-slicing
		ca.CARRIER_UID 
		, re.RESERVATIONFROM 
		, re.RESERVATIONTO 
		, CASE
			WHEN re.Drive_Minutes = 0 OR re.Drive_KM = 0 OR re.ReservationType_CTID > 1 THEN NULL
			WHEN re.Drive_FirstStart > re.ReservationFrom THEN re.Drive_FirstStart 
			ELSE re.ReservationFrom
			END																						AS DRIVEFROM
		, CASE
			WHEN re.Drive_Minutes = 0 OR re.Drive_KM = 0 OR re.ReservationType_CTID > 1 THEN NULL
			WHEN re.Drive_LastEnd < re.ReservationTo THEN re.Drive_LastEnd
			ELSE re.ReservationTo
			END																						AS DRIVETO
		, GREATEST(ca.BIZBEG, re.RESERVATIONFROM) AS RES_BIZBEG
		, LEAST(ca.BIZEND, re.RESERVATIONTO) AS RES_BIZEND
		, CASE WHEN ca.BIZBEG < DRIVETO AND ca.BIZEND > DRIVEFROM 
				THEN GREATEST(ca.BIZBEG, DRIVEFROM) ELSE NULL END 									AS DRIVE_BIZBEG
		, CASE WHEN ca.BIZBEG < DRIVETO AND ca.BIZEND > DRIVEFROM 
				THEN LEAST(ca.BIZEND, DRIVETO) ELSE NULL END 										AS DRIVE_BIZEND
		-- fields used after time-slicing
		, re.RESERVATION_BOID 
		, re.CONTRACT_BOID 
		, re.MAINABO_BOID 
		, re.PERSON_BOID 
		, re.RESERVATIONTYPE_CTID
		, re.ISTRANSFER		-- V3.2.2: Added
		, DATEDIFF(Minute, re.RESERVATIONFROM, re.RESERVATIONTO)									AS RESERVATION_MINUTES 
		, re.DRIVE_KM 
		, re.REVENUE_NET
	FROM {{ source('integrate_join', 'V_RESERVATION') }} re
		INNER JOIN {{ref('t_utilization_carrier')}}  ca
			ON	ca.CARRIER_BOID = re.CARRIER_BOID 
			AND	ca.BIZBEG < re.RESERVATIONTO 
			AND ca.BIZEND > re.RESERVATIONFROM 
			AND re.RESERVATIONFROM < re.RESERVATIONTO
	WHERE re.RESERVATIONSTATE_CTID != 11
),
CTE_utilization_restzeit
as 
(
	SELECT
		-- fields used for time-slicing
		ca.CARRIER_UID 
		, GREATEST(ca.BIZBEG, de.SERVICEFROM) AS BIZBEG
		, LEAST(ca.BIZEND, de.SERVICETO) AS BIZEND
		-- fields used after time-slicing 
		, de.CONTRACT_BOID 
		, de.PERSON_BOID 
	FROM {{ source('integrate_join', 'V_DEBT') }} de
		INNER JOIN {{ref('t_utilization_carrier')}}  ca
			ON	ca.CARRIER_BOID = de.CARRIER_BOID 
			AND	ca.BIZBEG < de.SERVICETO  
			AND ca.BIZEND > de.SERVICEFROM
			AND de.SERVICEFROM < de.SERVICETO
	WHERE de.COSTOBJECT_TYPE_CTID = 8 -- Restzeit Blockbuchungen
	QUALIFY ROW_NUMBER() OVER (PARTITION BY de.ServiceFrom, de.ServiceTo, ca.CARRIER_BOID ORDER BY de.Debt_BoID DESC) = 1

),
CTE_utilization_reservationCoat
as 
(
	SELECT
		ca.CARRIER_UID 
		, GREATEST(ca.BIZBEG, rc.BIZBEG) AS BIZBEG
		, LEAST(ca.BIZEND, rc.BIZEND) AS BIZEND
	FROM {{ source('integrate_join', 'V_RESERVATIONCOAT') }} rc
		INNER JOIN {{ref('t_utilization_carrier')}}  ca
			ON	ca.CARRIER_BOID = rc.CARRIER_BOID 
			AND	ca.BIZBEG < rc.BIZEND  
			AND ca.BIZEND > rc.BIZBEG
			AND rc.BIZBEG < rc.BIZEND
	WHERE rc.RESERVATIONCOATTYPEMAIN_CTID = 3 -- Sperrbuchungen (Zeiteinschraenkungen werden nicht beruecksichtigt)
),
CTE_utilization_offsettime
as 
(
	SELECT
		-- fields used for time-slicing
		ca.CARRIER_UID 
		, re.RESERVATION_BOID
		, GREATEST(ca.BIZBEG, re.RESERVATION_SYSTEMFROM)	AS BIZBEG 		-- OFFSET starts
		, LEAST(ca.BIZEND, re.RESERVATIONFROM)				AS BIZEND		-- OFFSET ends
	FROM {{ source('integrate_join', 'V_RESERVATION') }} re
		INNER JOIN {{ref('t_utilization_carrier')}}  ca
			ON	ca.CARRIER_BOID = re.CARRIER_BOID 
			AND	ca.BIZBEG < re.RESERVATIONFROM
			AND ca.BIZEND > re.RESERVATION_SYSTEMFROM
	WHERE re.RESERVATIONSTATE_CTID != 11	-- no cancelled reservations
		AND re.RESERVATION_SYSTEMFROM < re.RESERVATIONFROM		-- Reservations with offset-minutes
),
CTE_utilization_pointsInTime
as 
(
	SELECT distinct
		ca.CARRIER_UID
		, dt.BIZBEG		AS pointInTime
	FROM CTE_utilization_dateTime dt
		JOIN {{ref('t_utilization_carrier')}}  ca
			ON	dt.BIZBEG >= ca.BIZBEG
			AND dt.BIZBEG < ca.BIZEND
    
    union 

	SELECT distinct
		ca.CARRIER_UID
		, dt.BIZEND			AS pointInTime
	FROM CTE_utilization_dateTime dt
		JOIN {{ref('t_utilization_carrier')}}  ca
			ON	dt.BIZEND > ca.BIZBEG
			AND dt.BIZEND <= ca.BIZEND

    union 

	SELECT distinct
		ca.CARRIER_UID
		, ca.BIZBEG 		AS pointInTime
	FROM {{ref('t_utilization_carrier')}}  ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZEND 		AS pointInTime
	FROM {{ref('t_utilization_carrier')}}  ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZBEG 		AS pointInTime
	FROM CTE_utilization_vehicle ca

    union

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZEND 		AS pointInTime
	FROM CTE_utilization_vehicle ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.RES_BIZBEG 		AS pointInTime
	FROM CTE_utilization_reservation ca

    union

    SELECT distinct
		ca.CARRIER_UID
		, ca.RES_BIZEND 		AS pointInTime
	FROM CTE_utilization_reservation ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.DRIVE_BIZBEG 		AS pointInTime
	FROM CTE_utilization_reservation ca

    union

    SELECT distinct
		ca.CARRIER_UID
		, ca.DRIVE_BIZEND 		AS pointInTime
	FROM CTE_utilization_reservation ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZBEG 		AS pointInTime
	FROM CTE_utilization_restzeit ca

    union

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZEND 		AS pointInTime
	FROM CTE_utilization_restzeit ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZBEG 		AS pointInTime
	FROM CTE_utilization_reservationCoat ca

    union

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZEND 		AS pointInTime
	FROM CTE_utilization_reservationCoat ca

    union 

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZBEG 		AS pointInTime
	FROM CTE_utilization_offsettime ca

    union

    SELECT distinct
		ca.CARRIER_UID
		, ca.BIZEND 		AS pointInTime
	FROM CTE_utilization_offsettime ca
),
CTE_utilization_timeSlice
as 
(
	select
		Carrier_UID -- V1.9: replaced Carrier_SID as main key
		 , PointInTime																					AS BizBeg
		 , lead(PointInTime) over (partition by Carrier_UID order by PointInTime asc)					AS BizEnd
	from CTE_utilization_pointsInTime
	QUALIFY lead(PointInTime) over (partition by Carrier_UID order by PointInTime asc) > PointInTime -- V2.0: remove the last slice ending in eternity (BizEnd = null)
),
CTE_utilization_enrich
as 
(
	SELECT 
		x.Carrier_UID
		, ca.Base_BoID
		, ca.Municipality_ID																	AS MUNICIPALITY_ID -- V4.1 added
		, coalesce(re.Contract_BoID, rest.Contract_BoID, 0)										AS Contract_BoID -- V2.2 rest.Contract_SID added
		, ifnull(ma.MainAbo_UID, 0)																AS MainAbo_UID
		, coalesce(re.Person_BoID, rest.Person_BoID, 0)											AS Person_BoID -- V2.2 rest.Person_SID added
		, ifnull(re.Reservation_BoID, 0)														AS Reservation_BoID
		, ve.Vehicle_UID
		, DATE_TRUNC(DAY, x.BizBeg)																AS Date
		, HOUR(x.BizBeg)																		AS Hour
		
		-- V2.5 the sequence of the WHEN-Clauses defines which UtilisationType wins: it's the first match
		, CASE 
			WHEN re.ReservationType_CTID = 1			-- drive
				AND re.ISTRANSFER = 0	-- no Transfers
				and x.BizBeg >= re.Drive_BIZBEG
				and x.BizEnd <= re.Drive_BIZEND
				THEN 70		 
			WHEN re.ReservationType_CTID = 1 
				AND re.ISTRANSFER = 0	-- no Transfers
				THEN 60									-- reservation
			WHEN rest.Carrier_UID IS NOT NULL THEN 50	-- remain time
			WHEN re.ReservationType_CTID > 1 
				AND re.ISTRANSFER = 0	-- no Transfer
				THEN 40									-- service reservation
			WHEN re.ISTRANSFER = 1 THEN 35				-- Transfer reservation V3.2.2
			WHEN rc.Carrier_UID IS NOT NULL THEN 30		-- blocking ResCoat
			WHEN ro.Carrier_UID IS NOT NULL THEN 25		-- Offset time V3.2.2
			WHEN ve.Vehicle_UID IS NOT NULL THEN 20		-- Carrier with Vehicle
			ELSE 10										-- active Carrier
			END																					AS UtilizationID

		-- supply gross = minutes in the current slice
		, datediff(minute, x.BizBeg, x.BizEnd) / 60												AS Supply_Gross
		
		-- helpers to identify source of demand or supply-reduction
		, ifnull(re.ReservationType_CTID, 0)													AS ReservationType_CTID
		, case when rc.Carrier_UID is null then 0 else 1 end 									AS ResCoat -- V1.2: added
		, CASE WHEN rest.Carrier_UID is null then 0 else 1 end 									AS Restzeit -- V1.5: modified to ignore quantity

		-- target values of carriers
		, ca.Budget1 -- V1.9: added
		, ca.Budget2 -- V1.9: added 

		-- V2.5: Added fields from Drive_KM to SliceFactor, exclude ServiceReservations
		, CASE WHEN re.ReservationType_CTID = 1 THEN re.Drive_KM END							AS Drive_KM
		, CASE WHEN re.ReservationType_CTID = 1 THEN re.Revenue_Net END							AS Revenue_Net
		, CASE WHEN re.ReservationFrom = x.BizBeg 
			AND re.ReservationType_CTID = 1 THEN 1 ELSE 0 END									AS Res_Start
		, CASE WHEN re.ReservationTo = x.BizEnd 
			AND re.ReservationType_CTID = 1 THEN 1 ELSE 0 END									AS Res_End
		, CASE WHEN re.DriveFrom = x.BizBeg 
			AND re.ReservationType_CTID = 1 THEN 1 ELSE 0 END									AS Drive_Start
		, CASE WHEN re.DriveTo = x.BizEnd 
			AND re.ReservationType_CTID = 1 THEN 1 ELSE 0 END									AS Drive_End
		-- Factor to calculate the fraction of Drive_KM and Revenue_Net for this slice in the following step
		, CASE WHEN re.Reservation_Minutes > 0 
			THEN 	DATEDIFF(Minute, x.BizBeg, x.BizEnd)
					/ re.Reservation_Minutes 
			ELSE NULL END																		AS SliceFactor
	
	FROM CTE_utilization_timeSlice x

		-- get carrier in the current slice
		INNER JOIN {{ref('t_utilization_carrier')}}  ca
			ON	ca.CARRIER_UID = x.CARRIER_UID
			AND	ca.BIZBEG <= x.BIZBEG 
			AND	ca.BIZEND >= x.BIZEND 
	
		-- get vehicle in the current slice
		LEFT JOIN {{ source('integrate_key', 'T_VEHICLE') }} ve
			ON	ca.VEHICLE_BOID  = ve.VEHICLE_BOID
			AND	ve.BIZBEG <= x.BIZBEG				-- find the vehicle-slice for the current point in time 
			AND ve.BIZEND > x.BIZBEG
	
		-- get reservation in the current slice
		LEFT JOIN CTE_utilization_reservation re
			ON	re.CARRIER_UID = x.CARRIER_UID
			and re.RES_BIZBEG	<= x.BizBeg		-- reservations could be overlapping
			and re.RES_BIZEND >= x.BizEnd
	
		-- add mainAbo to reservation
		left join {{ source('integrate_key', 'T_MAINABO') }} ma
			on	ma.MAINABO_BOID = re.MAINABO_BOID
			and	ma.CONTRACT_BOID = re.CONTRACT_BOID
			and	ma.PERSON_BOID = re.PERSON_BOID 
			and	ma.BizBeg <= re.ReservationFrom		-- mainAbo should be valid at beginning of reservation 
			and	ma.BizEnd > re.ReservationFrom
	
		-- get Restzeit in the current slice
		LEFT JOIN CTE_utilization_restzeit rest
			ON	rest.CARRIER_UID = x.CARRIER_UID
			and rest.BizBeg <= x.BizBeg 		-- restzeit could be overlapping
			and rest.BizEnd >= x.BizEnd
	
		-- get resCoat in the current slice
		LEFT JOIN CTE_utilization_reservationCoat rc
			ON	rc.CARRIER_UID = x.CARRIER_UID
			and rc.BizBeg <= x.BizBeg				-- resCoat could be overlapping
			and rc.BizEnd >= x.BizBeg
			
		-- get resOffset in the current slice		V3.2.1 added
		LEFT JOIN CTE_utilization_offsettime ro
			ON	ro.CARRIER_UID = x.CARRIER_UID
			and ro.BizBeg <= x.BizBeg				
			and ro.BizEnd >= x.BizBeg
			
		
	QUALIFY row_number() OVER (PARTITION BY x.Carrier_UID, x.BizBeg ORDER BY re.ReservationType_CTID asc, re.Reservation_BoID desc) = 1 -- there could be two reservations in the same slice => only get one
		AND row_number() OVER (PARTITION BY x.Carrier_UID, x.BizBeg ORDER BY rc.BizEnd desc) = 1 -- there could be two resCoats in the same slice => only get one
		AND row_number() OVER (PARTITION BY x.Carrier_UID, x.BizBeg ORDER BY ro.RESERVATION_BOID desc) = 1 -- there could be two resOffsets in the same slice => only get one
),
CTE_utilization
as 
(
	SELECT
		x.Carrier_UID
		, x.Base_BoID
		, x.Contract_BoID	
		, x.MainAbo_UID	
		, x.Person_BoID	
		, x.Reservation_BoID	
		, x.Vehicle_UID
		, x.MUNICIPALITY_ID			-- V4.1 added
		, x.Date
		, x.Hour

		, x.Supply_Gross
		-- V2.5 Supply/Demand/Gross/Net is defined in meta.UtilisationType
		, CASE WHEN u.SUPPLY_NET THEN x.Supply_Gross ELSE 0 END						AS Supply_Net
		, CASE WHEN u.DEMAND_GROSS THEN x.Supply_Gross ELSE 0 END 					AS Demand_Gross
		, CASE WHEN u.DEMAND_NET THEN x.Supply_Gross ELSE 0 END						AS Demand_Net
		, (
				SELECT
					CAST(VALUE AS FLOAT) AS MINDATE
				FROM META.MODEL.T_CONFIGURATION
				WHERE UPPER(NAME) = 'TARGETINTERCEPT'
			) * (
			CASE WHEN u.SUPPLY_NET THEN x.Supply_Gross ELSE 0 END -- Supply_Net
			) 																		AS DemandTarget_Net -- V1.8: take @TargetIntercept * Supply_Net as default target, explicit calculation will follow below

		, x.Budget1 / (365*24) * (
			CASE WHEN u.SUPPLY_NET THEN x.Supply_Gross ELSE 0 END -- Supply_Net
			)																		AS Budget1 -- Budget1 * Supply_Net

		, x.Budget2 / (365*24) * (
			CASE WHEN u.SUPPLY_NET THEN x.Supply_Gross ELSE 0 END -- Supply_Net
			)																		AS Budget2 -- Budget2 * Supply_Net

		-- V2.5: New attributes and measures
		, u.UtilizationID
		, to_number(x.Drive_KM * x.SliceFactor, 38, 4)								AS Drive_KM
		, to_number(x.Revenue_Net * x.SliceFactor, 19, 4)							AS Revenue_Net
		, x.Res_Start
		, x.Res_End
		, x.Drive_Start
		, x.Drive_End
		
	FROM CTE_utilization_enrich x
		JOIN {{ source('meta_model', 'T_UTILIZATIONTYPE') }} u
			ON	u.UTILIZATIONID = x.UtilizationID		

)
select * from CTE_utilization
