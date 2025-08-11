-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Standard Examples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JSON to SQL

-- COMMAND ----------

SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE');


-- COMMAND ----------

SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));


-- COMMAND ----------

SELECT from_json('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>');

-- COMMAND ----------

SELECT from_json('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>');

-- COMMAND ----------

SELECT get_json_object('{"a":"b"}', '$.a');

-- COMMAND ----------

SELECT json_array_length('[1,2,3,4]');

-- COMMAND ----------

SELECT json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');

-- COMMAND ----------

SELECT json_array_length('[1,2');

-- COMMAND ----------

SELECT json_object_keys('{}');

-- COMMAND ----------

SELECT json_object_keys('{"key": "value"}');

-- COMMAND ----------

SELECT json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}');

-- COMMAND ----------

SELECT json_tuple('{"a":1, "b":2}', 'a', 'b');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema of JSON

-- COMMAND ----------

SELECT schema_of_json('[{"col":0}]');

-- COMMAND ----------

SELECT schema_of_json('[{"col":01}]', map('allowNumericLeadingZeros', 'true'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL to JSON

-- COMMAND ----------

SELECT to_json(named_struct('a', 1, 'b', 2));

-- COMMAND ----------

SELECT to_json(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));

-- COMMAND ----------

SELECT to_json(array(named_struct('a', 1, 'b', 2)));

-- COMMAND ----------

SELECT to_json(map('a', named_struct('b', 1)));

-- COMMAND ----------

SELECT to_json(map(named_struct('a', 1),named_struct('b', 2)));

-- COMMAND ----------

SELECT to_json(map('a', 1));

-- COMMAND ----------

SELECT to_json(array((map('a', 1))));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Relogix Specific Examples

-- COMMAND ----------

SELECT 
to_json
(
 collect_list
  (map("FloorId",FloorId
,"SpaceId",SpaceId
,"SpaceTypeId",SpaceTypeId
,"DayOfWeek",DayOfWeek
,"AverageDwellTime",AverageDwellTime
,"AverageNumberOfDwells",AverageNumberOfDwells
)))
from vizdata.SummaryDwellSpace 
where SpaceId  in (1015663,1024031)

-- COMMAND ----------

SELECT 
to_json(named_struct("customerid",customerid
,"FloorId",FloorId
,"SpaceId",SpaceId
,"SpaceTypeId",SpaceTypeId
,"StartInterval",StartInterval
,"HourOfDay",HourOfDay
,"DayOfWeek",DayOfWeek
,"Occupancy",Occupancy
,"Utilization",Utilization
,"Fullness",Fullness
,"DimDayOfWeekId",DimDayOfWeekId
,"SeatCapacity",SeatCapacity
,"AllocatedHeadCount",AllocatedHeadCount
,"FloorCapacity",FloorCapacity
)
)from vizdata.mainCallHourly  


-- COMMAND ----------

SELECT 
to_json(
  collect_list(
    map(       
  "CustomerId",CustomerId
  ,"FloorId",FloorId
  ,"HourOfDay",HourOfDay
  ,"Utilization",Utilization
    ))) from vizdata.avgsummaryhours  


-- COMMAND ----------

SELECT  
to_json(
--  collect_list(
    map(
       "customerid",customerid
      ,"FloorId",FloorId
      ,"SpaceId",SpaceId
      ,"SpaceTypeId",SpaceTypeId
      ,"StartInterval",StartInterval
      ,"DayOfWeek",DayOfWeek
      ,"Occupancy",Occupancy
      ,"Utilization",Utilization
      ,"Fullness",Fullness
      ,"DimDayOfWeekId",DimDayOfWeekId
      ,"SeatCapacity",SeatCapacity
      ,"AllocatedHeadCount",AllocatedHeadCount
      ,"FloorCapacity",FloorCapacity
    )
--  )
)
from vizdata.maincall  

-- COMMAND ----------

SELECT
to_json(
collect_list(  map(
  "FloorId", FloorId
  ,"SpaceId", SpaceId
  ,"AverageNumberOfDwells", AverageNumberOfDwells
  ))
)
from vizdata.peakSpaceByDayDwell 


-- COMMAND ----------

SELECT

to_json(
map("a",  collect_list(
  named_struct("dimFloorId",dimFloorId
,"FloorId",FloorId
,"AllocatedHeadCount",AllocatedHeadCount
,"FloorCapacity",FloorCapacity
,"param",param
))
))
from vizdata.dimFloorId_sample

-- COMMAND ----------

SELECT to_json(collect_list(map(
"CustomerId", CustomerId,
"FloorId", FloorId,
"HourOfDay", HourOfDay,
"Utilization", Utilization))) from vizdata.avgsummaryhours

-- COMMAND ----------

SELECT 
to_json(
  map(
  "CustomerId",CustomerId
  ,"FloorId",FloorId
  ,"SpaceId",`Space.SpaceId`
  ,"DayOfWeek_Utilization",`Space.DayOfWeek.Utilization`
  ,"DayOfWeek_Occupancy",`Space.DayOfWeek.Occupancy`
  ,"DayOfWeek_Fullness",`Space.DayOfWeek.Fullness`
  ,"PeakUtilization",PeakUtilization
  ,"AveragePeopleCount",AveragePeopleCount
  ,"PeakPeopleCount",PeakPeopleCount
  ,"FloorCapacity",FloorCapacity
  ,"AverageDwellTime",AverageDwellTime
  ,"AverageNumberOfDwells",AverageNumberOfDwells
  ))
 from vizdata.summary


-- COMMAND ----------

SELECT

to_json(
map("Floor",  collect_list(
  named_struct("FloorId",FloorId
,"CustomerId",CustomerId
,"AverageUtilization", coalesce(AverageUtilization,0)
,"AverageOccupancy", coalesce(AverageOccupancy,0)
,"AverageFullness", coalesce(AverageFullness,0.0)
,"AverageDeskDwellTime", coalesce(AverageDeskDwellTime,0.0)
,"AverageDeskNumberOfDwells", coalesce(AverageDeskNumberOfDwells,0.0)
,"AverageRoomDwellTime", coalesce(AverageRoomDwellTime,0.0)
,"AverageRoomNumberOfDwells", coalesce(AverageRoomNumberOfDwells,0.0)
))
))  from  vizdata.json_input

-- COMMAND ----------

-- SELECT 
--   table1.id,
--   to_json(collect_list(struct(table2.column1, table2.column2))) AS json_data
-- FROM
--   table1
-- JOIN
--   table2
-- ON
--   table1.id = table2.id
-- GROUP BY
--   table1.id
SELECT to_json(collect_list(
map("CustomerId", CAST(CustomerId as INT),
"FloorId", FloorId,
"HourOfDay", HourOfDay,
"Utilization", Utilization))) from vizdata.avgsummaryhours


-- COMMAND ----------


--CREATE OR REPLACE TABLE vizdata.json_input
--AS
WITH
  dimCustomerId
  AS
    (
    SELECT 
      `id` custkey
      , PartnerId CustomerId
    FROM 
      Customers_coalesce --delta.`/mnt/datalake/Mgmt/CustomerService/Customers` 
      dc
    WHERE dc.id = 62
    )
,
  dimfloorId
  AS
    (
      WITH tbl
      AS 
        (
            SELECT 
              `id` dimFloorId
              , `id` FloorId
              , AllocatedHeadCount
              , FloorCapacity
            FROM 
              --delta.`/mnt/datalake/Mgmt/SpaceService/Floors` 
              default.Floors_coalesce
              df
        )
      SELECT 
        * 
      FROM tbl
      LATERAL VIEW EXPLODE(SPLIT('355', ",")) AS param 
      --WHERE tbl.dimFloorId = CAST(param as INT)
    )
--select * from dimfloorID
    
,
  dimIntervalIdsHourly 
  AS
    (
    SELECT 
    dimintervalid
    , StartInterval
    , HOUR(StartInterval) HourOfDay
    FROM 
      tableau.dimintervals
                WHERE 
                  startinterval 
                  BETWEEN 
                      CAST('2019-01-01T00:00:00.000Z' AS TIMESTAMP) 
                  AND 
                    DATEADD(
                        MILLISECOND
                        , -10
                        , CAST(
                            DATEADD(
                                DAY
                                ,1
                                ,CAST ('2023-01-31T23:59:59.000Z' AS TIMESTAMP)) 
                            AS TIMESTAMP)
                    )
				AND 
                MINUTE(endinterval) = 0
			)
,
  dimintervalcategories
    AS
      (
        --TODO: REVAMP INtervalCategory to be sequence
      SELECT 
        dimIntervalCategoryId
      FROM 
        tableau.dimintervalcategory
  -- 				WHERE (1 IS NULL OR BusinessHour = 1)
  -- 					AND (1 IS NULL OR Weekday = 1)
  -- 					AND (0 IS NULL OR Holiday = 0)
  -- 					AND (NULL IS NULL OR Lunch = NULL)
      )
,
  dimSpaces
    AS
      (			
        WITH tbl
        AS 
        (
          SELECT 
            `id` SpaceId
            , `Name` SpaceType
            , SeatCapacity
            ,(
              HOUR(CAST(WorkHoursStartTime as TIMESTAMP))*60)
              +MINUTE(CAST(WorkHoursStartTime as TIMESTAMP)
              ) WorkHoursStartTimeInt
            ,(
              HOUR(CAST(WorkHoursEndTime as TIMESTAMP))*60)
              +MINUTE(CAST(WorkHoursEndTime as TIMESTAMP)
              ) WorkHoursEndTimeInt
              ,WorkHoursEndTime
              ,WorkHoursStartTime

          FROM 
              --delta.`/mnt/datalake/Mgmt/SpaceService/Spaces` 
              default.Spaces_coalesce st
        )
        SELECT 
          * 
        from 
          tbl
        LATERAL VIEW
            EXPLODE(
              SPLIT(
               '44329,44328,1024031,44334,44327,44344,1024032,1015655,1015663,1015656,44339'
               , ','
                  )
            ) AS param 
        WHERE 
          tbl.SpaceId = CAST(param as INT)
      )
,
  dimSpaceTypes 
    AS
      (
        WITH tbl
          AS
            (
              SELECT 
                `id` dimSpaceTypeId
                , `Name` SpaceType
              FROM
                --delta.`/mnt/datalake/Mgmt/SpaceService/SpaceTypes` 
                default.SpaceTypes_coalesce
                st
            )
              SELECT 
                * 
              FROM 
                tbl
                LATERAL VIEW  
                  EXPLODE(SPLIT('1', ',')) AS param 
              WHERE tbl.dimSpaceTypeId = CAST(param as INT)
      )
,
  dimDayOfWeek
    AS
      (
        WITH tbl
          AS 
          (
              SELECT 
                dimDayOfWeekId
                , `DayOfWeek`
              FROM 
                tableau.dimdayofweek ddow
          )
        SELECT 
          * 
        FROM 
          tbl
        LATERAL VIEW
          EXPLODE(
            SPLIT('2,3,4,5,6', ",")
          ) AS param 
      --  WHERE tbl.dimDayOfWeekId = CAST(param as INT)
      )
,
  dimDepartments
  AS
    (			
      WITH tbl
      AS 
          (
              SELECT 
                  id dimDepartmentId
                 , SpaceSubType `SpaceSubType`
                  , Department DepartmentName
              FROM 
                  --delta.`/mnt/datalake/Mgmt/SpaceService/SpaceProperties` 
                  default.SpaceProperties_coalesce
                  dd
                 WHERE IsActive = 1 
          )
      SELECT * from tbl
          LATERAL VIEW  
            EXPLODE(
              SPLIT('2,4', ',')
              ) AS param 
         -- WHERE tbl.DepartmentName = param
        )
,
	dimSpaceSubTypes 
		AS
			(			
              WITH tbl
              AS 
                (
                    SELECT 
                        `id` dimSpaceSubTypeId
                        ,  SpaceSubType SpaceSubTypeName
                    FROM 
                     default.SpaceProperties_coalesce
                        --delta.`/mnt/datalake/Mgmt/SpaceService/SpaceProperties` 
                        dd
                )
              SELECT * from tbl
                LATERAL VIEW  
                  EXPLODE(
                    SPLIT('2,4', ',')
                    ) AS param 
				--WHERE tbl.SpaceSubTypeName = param
			)
,
  mainCallHourly 
    AS
    (
        SELECT dc.customerid
                , df.FloorId
                , ds.SpaceId
                , dst.dimSpaceTypeId SpaceTypeId
                , StartIntervalUtc StartInterval
                , EndIntervalUtc EndInterval
                , BusinessHour HourOfDay
                , ddw.`DayOfWeek`
                , CASE WHEN AVG(fact.Utilization) > 0 THEN 1.0 ELSE 0.0 END Occupancy
                , AVG(fact.utilization) Utilization
                , CASE
                    WHEN dst.dimSpaceTypeId IN (2,4) THEN AVG(fact.Fullness) ELSE NULL
                END Fullness
                , ddw.DimDayOfWeekId
                , ds.SeatCapacity
                , df.AllocatedHeadCount
                , df.FloorCapacity
            FROM  --delta.`/mnt/datalake/VizData/AggHourSpace` 
              default.AggHourSpace_coalesce fact
                INNER JOIN dimCustomerId dc ON fact.customerid = dc.custkey
                INNER JOIN dimFloorId df ON fact.floorid = df.dimfloorid
                -- INNER JOIN 
                --   dimIntervalIdsHourly dil 
                --   ON fact.dimIntervalLocalId = dil.dimIntervalId
                -- INNER JOIN dimIntervalCategories ic 
                -- ON fact.dimintervalcategoryid = ic.dimintervalcategoryid
                INNER JOIN dimDayOfWeek ddw 
                  ON  
                    ddw.dimDayOfWeekId = dayofweek(fact.EndIntervalLocal)
                INNER JOIN dimSpaces ds 
                  ON fact.spaceid = ds.SpaceId
                LEFT JOIN dimSpaceTypes dst 
                  ON fact.SpaceTypeId = dst.dimSpaceTypeId
                LEFT JOIN dimDepartments dd 
                  ON fact.SpaceId = dd.dimDepartmentId
                LEFT JOIN dimSpaceSubTypes subt 
                  ON dd.SpaceSubType = subt.dimSpaceSubTypeId
            -- WHERE
            -- 	(
            -- 		(@LocalHourStartTemp IS NULL AND @LocalHourEndTemp IS NULL)  OR
            -- 		((@LocalHourStartTemp IS NULL AND @LocalHourEndTemp IS NOT NULL) AND (dil.HourOfDay BETWEEN 0 and @LocalHourEndTemp)) OR
            -- 		((@LocalHourStartTemp IS NOT NULL AND @LocalHourEndTemp IS NULL) AND (dil.HourOfDay BETWEEN @LocalHourStartTemp and 24)) OR
            -- 		(dil.HourOfDay BETWEEN @LocalHourStartTemp and @LocalHourEndTemp)
            -- 	)
            -- 	AND (@DepartmentNamesTemp IS NULL OR dd.dimDepartmentId IS NOT NULL)
            -- 	AND (@SpaceSubTypeNamesTemp IS NULL OR subt.dimSpaceSubTypeId IS NOT NULL)
            -- 	AND (@SpaceTypeIdsTemp IS NULL OR dst.dimSpaceTypeId IS NOT NULL)
            GROUP BY dc.customerid
                , df.FloorId
                , ds.SpaceId
                , StartIntervalUtc --StartInterval
                , EndIntervalUtc
                , HourOfDay
                , ddw.`DayOfWeek`
                , ddw.DimDayOfWeekId
                , dst.dimSpaceTypeId
                , ds.SeatCapacity
                , df.AllocatedHeadCount
                , df.FloorCapacity
    )
, 
	maincall
		AS	
			(
				SELECT 
					customerid
					, FloorId
					, SpaceId
					, SpaceTypeId
					, CAST(StartInterval AS DATE) StartInterval
					, CAST(EndInterval AS DATE) EndInterval
					, DayOfWeek
					, CASE WHEN AVG(Utilization) > 0 THEN 1.0 ELSE 0.0 END Occupancy
					, AVG(Utilization) Utilization
					, CASE
						WHEN SpaceTypeId IN (2,4) THEN AVG(Fullness) ELSE NULL
					END Fullness
					, DimDayOfWeekId
					, SeatCapacity
					, AllocatedHeadCount
					, FloorCapacity
					, COUNT(DISTINCT StartInterval) AvailableHours
				FROM mainCallHourly
				GROUP BY customerid
					, FloorId
					, SpaceId
					, CAST(StartInterval AS DATE)
          , CAST(EndInterval AS DATE)
					, DayOfWeek
					, DimDayOfWeekId
					, SpaceTypeId
					, SeatCapacity
					, AllocatedHeadCount
					, FloorCapacity
			)
,
	mainCallPeopleCount
		AS
			(
				SELECT 
        fact.customerid
				, fact.FloorId
        , fact.SpaceId
        , cast(StartIntervalUTC as DATE)  StartInterval
        , cast(EndIntervalUtc as DATE)  EndInterval
 				, ddw.`DayOfWeek` 
             --    , ddw.DimDayOfWeekId `DayOfWeek`
				, dst.dimSpaceTypeId SpaceTypeId
				, CASE 
                  WHEN SUM(CountBlocks) = 0 
                  THEN 0 
                  ELSE SUM(fact.InstancePeopleCount) / SUM(CountBlocks) 
                  END AveragePeopleCount
				, MAX(AveragePeopleCount) PeakPeopleCount
				, ds.SeatCapacity

				-- select *
			FROM  -- delta.`dbfs:/mnt/datalake/VizData/PeopleCountSpaceHourly` 
          default.PeopleCountSpaceHourly_coalesce fact
				INNER JOIN dimCustomerId dc 
                  ON fact.customerid = 62 --dc.custkey
				INNER JOIN dimFloorId df 
                  ON fact.floorid = 355 --df.dimfloorid
				-- INNER JOIN dimIntervalIdsHourly dil 
        --         ON fact.dimIntervalLocalId = dil.dimIntervalId
				-- INNER JOIN dimIntervalCategories ic 
        --           ON fact.dimintervalcategoryid = ic.dimintervalcategoryid
				INNER JOIN dimDayOfWeek ddw 
                  ON ddw.`DayOfWeek` = dayofweek(fact.EndIntervalLocal)
				INNER JOIN dimSpaces ds 
                  ON fact.spaceid = ds.SpaceId
				LEFT JOIN dimSpaceTypes dst 
                  ON fact.SpaceTypeId = dst.dimSpaceTypeId
				LEFT JOIN dimDepartments dd 
                  ON fact.SpaceId = dd.dimDepartmentId
                LEFT JOIN dimSpaceSubTypes subt 
                  ON dd.SpaceSubType = subt.dimSpaceSubTypeId
			-- WHERE
			-- 	(@DepartmentNamesTemp IS NULL OR dd.dimDepartmentId IS NOT NULL)
			-- 	AND (@SpaceSubTypeNamesTemp IS NULL OR subt.dimSpaceSubTypeId IS NOT NULL)
			-- 	AND (@SpaceTypeIdsTemp IS NULL OR dst.dimSpaceTypeId IS NOT NULL)
			GROUP BY fact.customerid
				, fact.FloorId
				, fact.SpaceId
--				, fact.`Date`  --CAST(StartInterval as DATE)
				, ddw.`DayOfWeek`
 				--, ddw.DimDayOfWeekId
				, dst.dimSpaceTypeId
				, ds.SeatCapacity
                , cast(StartIntervalUTC as DATE)  
        , cast(EndIntervalUtc as DATE)  
			)
,			
	mlist
		AS
			(
				SELECT 
                  customerid
                  , floorid
                  , spaceid 
                  , SpaceTypeId
                  , StartInterval
                  , `DayOfWeek`
				FROM maincall
				GROUP BY 
                  customerid
                  , floorid
                  , spaceid 
                  , SpaceTypeId
                  , StartInterval
                  , `DayOfWeek`
				UNION ALL
				SELECT 
                  customerid
                  , floorid
                  , spaceid 
                  , SpaceTypeId
                  , StartInterval
                  , `DayOfWeek`
				FROM maincallpeoplecount
				GROUP BY
                  customerid
                  , floorid
                  , spaceid 
                  , SpaceTypeId
                  , StartInterval
                  , `DayOfWeek`
			)
,
	FloorsDate
		AS
        (	
          SELECT 
            CustomerId
            , FloorId
            , DayOfWeek
            , AllocatedHeadCount 
            , StartInterval
          FROM mainCall
          GROUP BY
            CustomerId
            , FloorId
            , DayOfWeek
            , AllocatedHeadCount 
            , StartInterval
        )
,
	masterList
		AS
		(
			SELECT customerid, floorid, spaceid, SpaceTypeId
            , StartInterval
                  , `DayOfWeek`
			FROM mlist
			UNION ALL
			-- SpaceID is null since floors don't have spaces
			SELECT customerid, floorid, null,null
            , StartInterval
                  , `DayOfWeek`
			FROM floorsdate
		)
,
	Floors
	(
		SELECT 
          CustomerId
          , FloorId
                  , `DayOfWeek`
          , AllocatedHeadCount
		FROM mainCall
		GROUP BY 
          CustomerId
          , FloorId
                  , `DayOfWeek`
          , AllocatedHeadCount
-- 		UNION ALL
-- 		SELECT CustomerId, FloorId, DayOfWeek, AllocatedHeadCount
-- 		FROM maincallFloorPeopleCount
-- 		GROUP BY CustomerId, FloorId, DayOfWeek, AllocatedHeadCount
	)

,
	summaryPeopleCountSpace
		AS
			(
				SELECT
					CustomerId,
					FloorId,
					mc.SpaceId
					,SpaceTypeId
                  , mc.`DayOfWeek`
					--,DimDayOfWeekId,
					,SeatCapacity,
					AVG(AveragePeopleCount) AveragePeopleCount,
					MAX(PeakPeopleCount) PeakPeopleCount
				FROM mainCallPeopleCount mc
				GROUP BY CustomerId,
					FloorId,
					mc.SpaceId
					,SpaceTypeId
                  , mc.`DayOfWeek`
					--,DimDayOfWeekId
					,SeatCapacity
                -- DISTRIBUTE BY
                --     floorid
                --     ,spaceid
                --     ,`DayOfWeek`

			)
, summaryDwellSpace
AS
(
      SELECT FloorId, SpaceId, SpaceTypeId
      , StartInterval
      , `DayOfWeek`, AverageDwellTime, AverageNumberOfDwells
      FROM 
        (
        SELECT mc.FloorId
                  , mc.SpaceId
                  , mc.SpaceTypeId
                  , mc.StartInterval 
                  , mc.`DayOfWeek`
  -- keep as null instead of 0 because we dont want to factor in non-used rooms in avg calc
                  , CASE
                    WHEN 
                    (
                      AverageDwellMinutes IS NULL 
                      AND 
                      mc.Utilization = 0
                      )
                    THEN NULL 
                          ELSE
  -- If avg daily utilization=100%
  --, then someone was always there,dwelltime=dailyusage minutes
                              CASE
                              WHEN 
                                (
                                  AverageDwellMinutes IS NULL 
                                  AND 
                                  mc.Utilization = 1
                                )
                              THEN mc.AvailableHours * mc.Utilization * 60
                              ELSE
  -- few test cases had this case occur. Utilization was very low (0.009-0.07)
                                  CASE
                                    WHEN 
                                    (
                                      AverageDwellMinutes IS NULL 
                                      AND 
                                      mc.Utilization != 0
                                    )
                                    THEN 0 --average dwell = 0
                                    ELSE AverageDwellMinutes -- normal cases
                                    END
                              END
                  END AverageDwellTime
                  , COALESCE(CountOccupied, 1) AverageNumberOfDwells
        FROM 
        (
          SELECT FloorId
              , SpaceId
              , Date
              , f.`DayOfWeek`
              , AVG(
                    DATEDIFF(
                      SECOND
                      , f.DateTimeStartLocal
                      , f.DateTimeEndLocal
                      ) 
                      /60.0
                    
        ) AverageDwellMinutes
        , CAST(
            SUM(
              CASE 
              WHEN state = 1 
              THEN 1 
              ELSE 0 
              END) 
            AS DECIMAL(12, 5)
          )                  CountOccupied
          FROM (
                  SELECT dc.FloorId
                          , dc.SpaceId
                          , dc.date
                          , dc.State
                          ,DATE_FORMAT(Date, 'EEEE') `DayOfWeek`
                      --     , CASE
                      --         WHEN (HOUR(dateTimeStartLocal)*60)+MINUTE(dateTimeStartLocal) < WorkHoursStartTime
                      --             THEN WorkHoursStartTime
                      --         ELSE dateTimeStartLocal
                      -- END                        DateTimeStartLocal
                      --     , CASE
                      --         WHEN (HOUR(dateTimeStartLocal)*60)+MINUTE(dateTimeStartLocal) > WorkHoursEndTime
                      --             --OR dateTimeEndLocal  = '00:00:00.0000000'
                      --             THEN WorkHoursEndTime
                      --         ELSE dateTimeEndLocal 
                      -- END                        DateTimeEndLocal

                          , CASE
                              WHEN CAST(dateTimeStartLocal AS TIMESTAMP) < WorkHoursStartTime
                                  THEN WorkHoursStartTime
                              ELSE CAST(dateTimeStartLocal AS TIMESTAMP)
                      END                        DateTimeStartLocal
                          , CASE
                              WHEN CAST(dateTimeEndLocal AS TIMESTAMP) > WorkHoursEndTime
                                  OR CAST(dateTimeEndLocal AS TIMESTAMP) = '00:00:00.0000000'
                                  THEN WorkHoursEndTime
                              ELSE CAST(dateTimeEndLocal AS TIMESTAMP)
                      END                        DateTimeEndLocal
                  FROM tableau.StateChangeDaily dc
                              INNER JOIN dimSpaces ds on dc.spaceid = ds.spaceid
                              INNER JOIN dimFloorId df on dc.FloorId = df.FloorId
                  WHERE 
                  -- (date BETWEEN CAST(@StartIntervalTemp AS DATE) AND CAST(@EndIntervalTemp AS DATE))
                  -- 	AND 
                      state = 1
                      AND dateTimeEndLocal  > WorkHoursStartTime
                      AND dateTimeStartLocal < WorkHoursEndTime
                      
                      AND CAST(dateTimeEndLocal AS TIMESTAMP) > WorkHoursStartTime
                      AND CAST(dateTimeStartLocal AS TIMESTAMP) < WorkHoursEndTime
              ) f
                  INNER JOIN dimDayOfWeek ddow ON ddow.DayOfWeek = f.DayOfWeek
          GROUP BY floorId, spaceId, date, f.DayOfWeek
      ) dwells
                      RIGHT JOIN mainCall mc ON --right join ensures we have utilization data to report
                          dwells.SpaceId = mc.SpaceId
                      --AND dwells.Date = mc.StartInterval
                      LEFT JOIN dimSpaces ds ON ds.SpaceId = mc.SpaceId
          ) dailyDwellAndChurn
      WHERE AverageDwellTime IS NOT NULL
      DISTRIBUTE BY spaceid 
  )

  ,
      peakSpaceByDayDwell 
      AS
          (
              SELECT 
                a.FloorId
                , a.SpaceId
                , a.`DayOfWeek`
                , a.AverageNumberOfDwells
              FROM (
                  SELECT FloorId, `DayOfWeek`, AverageNumberOfDwells, SpaceId
                  , ROW_NUMBER() OVER (PARTITION BY DayOfWeek ORDER BY AverageNumberOfDwells DESC) rank
                  FROM summaryDwellSpace) a
              WHERE a.rank = 1
		)
--select * from 	peakSpaceByDayDwell
--41.16 seconds w/o rangebin, 10.58 second time

--this table shape is designed to help assist the JSON output. Since the JSON parser will auto nest joined tables, to get fields in the right level in the response means bundling them together before you JSONize the output
,    pc
        AS
            (
                    SELECT 
                      SeatCapacity
                      , AveragePeopleCount
                      , PeakPeopleCount
                      , customerid
                      , floorid
                      , spaceid
                     , spacetypeid
                      , `DayOfWeek`
                      --, f.DimDayOfWeekId
                    FROM summaryPeopleCountSpace f
                ) 
, maincallagg
    AS
        (
            SELECT customerid
                , FloorId
                , SpaceId
                , SpaceTypeId
                , StartInterval
                , `DayOfWeek`
               -- , DimDayOfWeekId
                , AVG(Occupancy) Occupancy
                , AVG(Utilization) Utilization
                , AVG(Fullness) Fullness
                , SeatCapacity
                , FloorCapacity
            FROM mainCall
            GROUP BY customerid
                , StartInterval
                , `DayOfWeek`
                , FloorId
                , SpaceId
                , DimDayOfWeekId
                , SpaceTypeId
                , SeatCapacity
                , FloorCapacity
        )
,
    occ
        AS
            (
                    SELECT customerid
                        , d.FloorId
                        , d.SpaceId
                        , d.SpaceTypeId
                        , d.StartInterval
                        , d.`DayOfWeek`
 --                       , DimDayOfWeekId
                        , Occupancy
                        , Utilization
                        , Fullness
                        , MAX(Utilization) OVER(PARTITION BY d.SpaceId) PeakUtilization
                        , MAX(fullness) OVER(PARTITION BY d.SpaceId) PeakFullness
                        , MAX(Occupancy) OVER(PARTITION BY d.SpaceId) PeakOccupancy
                        , SeatCapacity
                        , AverageDwellTime, AverageNumberOfDwells
                        , FloorCapacity
                    FROM maincallagg d
                    LEFT JOIN 
                      SummaryDwellSpace 
                      ON 
                        d.SpaceId = SummaryDwellSpace.SpaceId 
                        AND d.StartInterval = SummaryDwellSpace.StartInterval
                    -- DISTRIBUTE BY
                    --     CustomerId
                    --     ,FloorId
                    --     ,spaceid 
                    --     , d.`DayOfWeek`
 --                       , DimDayOfWeekId
                )
--select * from occ --10.57 minutes 

,        
    peak
        AS
          (
        --/*
            SELECT 
              qlist.customerid
            , qlist.floorid
            , qlist.spaceid
            , qlist.StartInterval
            ,qlist.SpaceTypeId
                -- , COALESCE(occ.spacetypeid, pc.spacetypeid) SpaceTypeId
                , COALESCE(occ.`DayOfWeek`, pc.`DayOfWeek`) `DayOfWeek`
                , COALESCE(occ.SeatCapacity, pc.SeatCapacity) SeatCapacity
                , Occupancy
                , Utilization
                , Fullness
                , PeakUtilization
                , PeakFullness
                , PeakOccupancy
                , AveragePeopleCount
                , PeakPeopleCount
                , AverageDwellTime
                , AverageNumberOfDwells
                , FloorCapacity
            FROM  masterlist qlist
            LEFT JOIN occ 
                ON 
                  qlist.CustomerId = occ.CustomerId 
                  AND 
                  qlist.FloorId = occ.FloorId 
                  AND 
                  qlist.spaceid = occ.spaceid 
                  AND qlist.StartInterval = occ.StartInterval
            LEFT JOIN pc 
                  ON 
                    qlist.CustomerId = pc.CustomerId 
                    AND 
                    qlist.FloorId = pc.FloorId 
                    AND 
                    qlist.SpaceId = pc.SpaceId 
                    AND qlist.dayofweek = pc.dayofweek
              -- DISTRIBUTE BY
              --   qlist.floorid
              --   ,qlist.spaceid
              --   ,qlist.dayofweek 
              ) 
,
    cs
        AS
        (
                      SELECT
              peak.customerid
              , peak.FloorId
              , peak.SpaceId
              , peak.SpaceTypeId
              , peak.`DayOfWeek` 
            -- , peak.DimDayOfWeekId
              , peak.PeakUtilization
              , peak.PeakFullness
              , peak.PeakOccupancy
              , AVG(Utilization) Utilization
              , AVG(Occupancy) Occupancy
              , AVG(Fullness) Fullness
--               , SummaryPeopleCount.AveragePeopleCountFloor
--               , SummaryPeopleCount.PeakPeopleCountFloor
              , peak.SeatCapacity
              , SummaryPeopleCountSpace.AveragePeopleCount
              , SummaryPeopleCountSpace.PeakPeopleCount
              --, SummaryPeopleCount.AllocatedHeadCount
              , FloorCapacity
              , AVG(AverageDwellTime) AverageDwellTime
              , AVG(AverageNumberOfDwells) AverageNumberOfDwells
              --	select *
          FROM peak
--           LEFT JOIN summaryPeopleCountFloor SummaryPeopleCount 
--               ON 
--                   (
--                       -- @DepartmentNamesTemp IS NULL 
--                       -- AND 
--                       -- 	@SpaceSubTypeNamesTemp IS NULL 
--                       -- AND 
--                           peak.floorid = SummaryPeopleCount.FloorId 
--                       AND 
--                           peak.CustomerId = SummaryPeopleCount.CustomerId 
--                       AND 
--                           peak.DayOfWeek = SummaryPeopleCount.DayOfWeek
--                       )
          LEFT JOIN summaryPeopleCountSpace SummaryPeopleCountSpace 
              ON 
                      peak.floorid = SummaryPeopleCountSpace.floorid 
                  AND 
                      peak.spaceid = SummaryPeopleCountSpace.spaceid 
--                   AND 
--                       peak.DayOfWeek = SummaryPeopleCountSpace.DayOfWeek

          GROUP BY peak.customerid
              , peak.`DayOfWeek`
              , peak.FloorId
              , peak.SpaceId
               --, peak.DimDayOfWeekId
             , peak.SpaceTypeId
              , peak.PeakUtilization
              , peak.PeakFullness
              , peak.PeakOccupancy
--               , SummaryPeopleCount.AveragePeopleCountFloor
--               , SummaryPeopleCount.PeakPeopleCountFloor
              , peak.SeatCapacity
              , SummaryPeopleCountSpace.AveragePeopleCount
              , SummaryPeopleCountSpace.PeakPeopleCount
--               , SummaryPeopleCount.AllocatedHeadCount
              , FloorCapacity
      )              
,
  Summary
    AS
      (
        SELECT
          customerid CustomerId
          , cs.FloorId -- as `FloorId`
          , cs.SpaceId as  `Space.SpaceId`
          , cs.SpaceTypeId
          , cs.`DayOfWeek` as `DayOfWeek`
          , cs.Utilization as `Utilization`
          , Occupancy as `Occupancy`
          , Fullness as `Fullness`
          --, DimDayOfWeekId
          , PeakUtilization
          , PeakFullness
          , PeakOccupancy
--           , AveragePeopleCountFloor
--           , PeakPeopleCountFloor
          , SeatCapacity
          , AveragePeopleCount
          , PeakPeopleCount
          --, AllocatedHeadCount
          , FloorCapacity
          , AverageDwellTime
          , AverageNumberOfDwells
      FROM  cs
  )
,
	avgsummaryhours 
		AS
			(
				SELECT spaces.CustomerId
					, spaces.FloorId
					, Summary.HourOfDay
					, AVG(Summary.Utilization) AS Utilization
				FROM Summary spaces
				INNER JOIN (
					--changes to replace the old hourlySpaces
					--instead of having a temp table, just calculate the
					--hourly Utilization per space here
					SELECT CustomerId
						, FloorId
						, HourOfDay
						, AVG(Utilization) AS Utilization
					FROM mainCallHourly
					GROUP BY CustomerId, FloorId, SpaceId, HourOfDay
				) summary ON spaces.FloorId = summary.Floorid
				GROUP BY spaces.customerid
					, spaces.Floorid
					, summary.hourofday
			)
--select * from avgsummaryhours
, json_input as
(	SELECT Floors.FloorId
				, Floors.CustomerId
			    , Floors.AverageDailyUtilization
			    , Floors.AverageDailyOccupancy
			    , Floors.AverageDailyFullness
				, Floors.PeakUtilization
				, Floors.PeakOccupancy
				, Floors.PeakFullness
        ,Summary.SpaceTypeId 
				--, Floors.AllocatedHeadCount
				, Floors.FloorCapacity
				--, CEILING(Floors.PeakPeopleCountFloor) PeakPeopleCount
				--, CEILING(Floors.AveragePeopleCountFloor) AveragePeopleCount
				, Floors.AverageDwellTime
				, Floors.AverageRoomDwellTime
				, Summary.`DayOfWeek`
				, AVG(Summary.utilization) `AverageUtilization`
				, AVG(Summary.Occupancy) `AverageOccupancy`
				, AVG(CASE WHEN summary.SpaceTypeId IN (2,4) THEN Summary.Fullness ELSE NULL END) `AverageFullness`
				-- , CEILING(Summary.AveragePeopleCountFloor) `AveragePeopleCount`
				-- , CEILING(Summary.PeakPeopleCountFloor) `PeakPeopleCount`
				, CAST(AVG(CASE WHEN Summary.SpaceTypeId IN (1,3) THEN Summary.AverageDwellTime ELSE NULL END) AS DECIMAL(12,0)) `AverageDeskDwellTime`
				, AVG(CASE WHEN Summary.SpaceTypeId IN (1,3) THEN Summary.AverageNumberOfDwells ELSE NULL END) `AverageDeskNumberOfDwells`
				, CAST(AVG(CASE WHEN Summary.SpaceTypeId IN (2,4) THEN Summary.AverageDwellTime ELSE NULL END) AS DECIMAL(12,0)) `AverageRoomDwellTimebySpace`
				, AVG(CASE WHEN Summary.SpaceTypeId IN (2,4) THEN Summary.AverageNumberOfDwells ELSE NULL END) `AverageRoomNumberOfDwells`
				-- , MAX(CASE WHEN summary.SpaceTypeId IN (2,4) THEN PeakDwell.PeakSpaceId ELSE NULL END) `PeakRoomId`
				-- , MAX(CASE WHEN summary.SpaceTypeId IN (2,4) THEN PeakDwell.PeakNumberOfDwells ELSE NULL END) `PeakRoomNumberOfDwells`
			FROM (
				SELECT
					floorcalc.customerid
					, floorcalc.floorid
					, floorcalc.`dayofweek`
					, FloorAverages.AverageDailyUtilization
					, FloorAverages.AverageDailyOccupancy
					, FloorAverages.AverageDailyFullness
					, FloorPeaks.PeakUtilization
					, FloorPeaks.PeakOccupancy
					, FloorPeaks.PeakFullness
					--, AllocatedHeadCount
					--, PeakPeopleCountFloor
					--, AveragePeopleCountFloor
					, FloorDwell.AverageDwellTime
					, FloorDwell.AverageRoomDwellTime
					, floorCapacity.FloorCapacity FloorCapacity
				FROM (
						SELECT
							CustomerId, FloorId,summary.`DayOfWeek`
							-- , COALESCE(AllocatedHeadCount, 0) AllocatedHeadCount
							-- , AVG(averagePeopleCountFloor) OVER() AveragePeopleCountFloor
							-- , MAX(PeakPeopleCountFloor) OVER() PeakPeopleCountFloor
						FROM summary 
						WHERE summary.`Space.SpaceId` IS NULL
					) floorCalc
					LEFT JOIN (
							SELECT
								FloorId
								, CAST(AVG(CASE WHEN SpaceTypeId IN (1,3,5) THEN AverageDwellTime ELSE NULL END) AS DECIMAL(12,0)) `AverageDwellTime`
								, CAST(AVG(CASE WHEN SpaceTypeId IN (2,4) THEN AverageDwellTime ELSE NULL END) AS DECIMAL(12,0)) `AverageRoomDwellTime`
							FROM SummaryDwellSpace
							GROUP BY
								FloorId
						) FloorDwell ON floorCalc.FloorId = FloorDwell.FloorId
					LEFT JOIN (
							SELECT FloorId, FloorCapacity
							FROM dimFloorId
					) floorCapacity ON floorCalc.FloorId = floorCapacity.FloorId
			        LEFT JOIN (
			                SELECT FloorId
			                       , CAST(AVG(Utilization) AS DECIMAL(12,5)) AverageDailyUtilization
			                       , CAST(AVG(Occupancy) AS DECIMAL(12,5)) AverageDailyOccupancy
			                       , CAST(AVG(Fullness) AS DECIMAL(12,5)) AverageDailyFullness
			                FROM mainCall
			                GROUP BY FloorId
                    ) FloorAverages ON FloorAverages.FloorId = floorCalc.FloorId
			        LEFT JOIN (
			            	SELECT FloorId
			            	     , CAST(MAX(Occupancy) AS DECIMAL(12,5)) PeakOccupancy
			            	     , CAST(MAX(Utilization) AS DECIMAL(12,5)) PeakUtilization
			            	     , CAST(MAX(Fullness) AS DECIMAL(12,5)) PeakFullness
				            FROM (
						        SELECT FloorId, AVG(Occupancy) Occupancy, AVG(Utilization) Utilization, AVG(Fullness) Fullness
						        FROM maincallHourly
						        GROUP BY FloorId, StartInterval
					        ) hourlyAverages
	                        GROUP BY FloorId
                    ) FloorPeaks ON FloorPeaks.FloorId = floorCalc.FloorId
				) Floors
				LEFT JOIN summary Summary ON floors.floorid = Summary.floorid AND floors.customerid = Summary.customerid AND floors.`dayofweek` = Summary.`dayofweek`
				LEFT JOIN peakSpaceByDayDwell PeakDwell ON Floors.floorid = PeakDwell.FloorId AND Floors.`dayofweek` = PeakDwell.`dayofweek`

			GROUP BY
				Floors.FloorId, Floors.CustomerId, Summary.`DayOfWeek`
        ,Summary.SpaceTypeId 
				, Floors.AverageDailyUtilization
        , Floors.AverageDailyOccupancy
        , Floors.AverageDailyFullness
				, floors.PeakUtilization
        , floors.PeakOccupancy
        , floors.PeakFullness
				, Floors.AverageDwellTime
        , Floors.AverageRoomDwellTime
--				, Summary.AveragePeopleCountFloor, Summary.PeakPeopleCountFloor
				--, Floors.AllocatedHeadCount
--				, Floors.PeakPeopleCountFloor, Floors.AveragePeopleCountFloor
				--, PeakDwell.PeakSpaceId , PeakDwell.PeakNumberOfDwells
        , Floors.FloorCapacity
) 
SELECT

to_json(
map("Floor",  collect_list(
  named_struct("FloorId",FloorId
,"CustomerId",CustomerId
,"AverageDailyUtilization",coalesce(AverageDailyUtilization,0.0)
,"AverageDailyOccupancy",coalesce(AverageDailyOccupancy,0.0)
,"AverageDailyFullness",coalesce(AverageDailyFullness,0.0)
,"PeakUtilization",coalesce(PeakUtilization,0.0)
,"PeakOccupancy",coalesce(PeakOccupancy,0.0)
,"PeakFullness",coalesce(PeakFullness,0.0)
,"FloorCapacity",coalesce(FloorCapacity,0.0)
,"AverageDwellTime",coalesce(AverageDwellTime,0.0)
,"AverageRoomDwellTime",coalesce(AverageRoomDwellTime,0.0)
,"DayOfWeek",coalesce(DayOfWeek,0.0)
,"AverageUtilization", coalesce(AverageUtilization,0.0)
,"AverageOccupancy", coalesce(AverageOccupancy,0.0)
,"AverageFullness", coalesce(AverageFullness,0.0)
,"AverageDeskDwellTime", coalesce(AverageDeskDwellTime,0.0)
,"AverageDeskNumberOfDwells", coalesce(AverageDeskNumberOfDwells,0.0)
,"AverageRoomDwellTime", coalesce(AverageRoomDwellTime,0.0)
,"AverageRoomNumberOfDwells", coalesce(AverageRoomNumberOfDwells,0.0)
))
))  from  vizdata.json_input

-- COMMAND ----------


