# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
                create table if not exists f1_presentation.calculated_race_results
                (
                race_year int,
                team_name string,
                driver_id int,
                driver_name string,
                race_id int,
                position int,
                points int,
                calculated_points int,
                created_date timestamp,
                updated_date timestamp
                )
                using delta
""")

# COMMAND ----------

spark.sql(f"""
            create or replace temp view race_results_updated
            as
            select races.race_year
                    ,constructor.name as team_name
                    ,drivers.driver_id
                    ,races.race_id
                    ,results.points
                    ,results.position
                    ,(11-results.position) as calculated_points 
            from f1_processed.results
              inner join f1_processed.drivers on (results.driver_id=drivers.driver_id)
              inner join f1_processed.constructor on (results.constructor_id=constructor.constructor_id)
              inner join f1_processed.races on (results.race_id=races.race_id)
            where results.position <= 10
                  and results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
            MERGE INTO f1_presentation.calculated_race_results tgt
            USING race_results_updated upd
            ON tgt.driver_id = upd.driver_id and tgt.race_id=upd.race_id
            WHEN MATCHED THEN
              UPDATE SET tgt.position = upd.position,
                        tgt.points = upd.points,
                        tgt.calculated_points = upd.calculated_points,
                        tgt.updated_date = current_timestamp
            when not matched then
              insert (race_year, team_name, driver_id, race_id, points, position, calculated_points, created_date) 
              values (race_year, team_name, driver_id, race_id, points, position, calculated_points, current_timestamp)
""")

# COMMAND ----------


