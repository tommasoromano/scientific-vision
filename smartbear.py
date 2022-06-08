import pandas as pd
import numpy as np
import datetime
import pyspark.sql.dataframe
import pyspark.sql.session
import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import missingno as msno
import echart
import imputation

class sb_spark:
    '''
    A simple class to use pySpark and get Dataframes
    and generate JSON files for Echart
    - patients
    - conditions
    - observations
    - questionnaire

    @tommasoromano
    '''
    app_name:str
    db_path:str
    spark:pyspark.sql.session.SparkSession
    observations:pyspark.sql.dataframe.DataFrame
    conditions:pyspark.sql.dataframe.DataFrame
    patients:pyspark.sql.dataframe.DataFrame
    questionnaire:pyspark.sql.dataframe.DataFrame


    def __init__(self, app_name:str="name", db_path:str="db"):
        self.app_name = app_name
        self.db_path = db_path
        self.spark = SparkSession.builder.appName(self.app_name).enableHiveSupport().getOrCreate()
        self.observations = self.spark.read.format("parquet").load(self.db_path+'/observations')
        self.conditions = self.spark.read.format("parquet").load(self.db_path+'/conditions')
        self.patients = self.spark.read.format("parquet").load(self.db_path+'/patients')
        self.questionnaire = self.spark.read.format("parquet").load(self.db_path+'/questionnaire_responses')

    PATIENT='Patient_id'
    SUBJECT='subject_reference'
    CODE='code_coding_code'
    CMP_CODE='component_code_coding_code'
    VALUE='valueQuantity_value'
    CMP_VALUE='component_valueQuantity_value'
    TIME='effectiveDateTime'
    LOCAL_TIME='local_time'
    TIMESTAMP='timestamp'

    ################################################################
    #
    #   UTILS
    #
    ################################################################
    #region UTILS

    def bruteforce_find(self,to_find:str,dfIndex=-1):
        dfs = [self.observations, self.conditions, self.patients, self.questionnaire]
        for d in dfs:
            rows = d.collect()
            for r in rows:
                for c in r:
                    if to_find.upper() in str(c).upper():
                        print('FOUND\n\n\n\n',r)
                        raise KeyboardInterrupt
        print("Not Found")

    def df_column_to_list(self, df:pyspark.sql.dataframe.DataFrame, col_name, distinct=False)->list:
        '''
        - df
        - col_name: default=Patient_id

        Returns the list of col_name
        '''
        if distinct:
            return list(df.select(col_name).distinct().toPandas()[col_name])
        else:
            return list(df.select(col_name).toPandas()[col_name])

    def get_all(self, 
        df:pyspark.sql.dataframe.DataFrame, 
        col_name):
        '''
        Returns a list of all distinct values of a row of  DataFrame
        '''
        df = df.select(col_name)
        df = df.distinct()
        return self.df_column_to_list(df, col_name)

    def get_alls(self,
        df:pyspark.sql.dataframe.DataFrame, 
        col_names):
        df = df.select(col_names)
        df = df.groupBy(col_names).count()
        return df.orderBy(F.col('count').desc())

    def filter(self,
        df:pyspark.sql.dataframe.DataFrame,
        **kwargs
    )->pyspark.sql.dataframe.DataFrame:
        '''
        A Shortcut for filtering by values and None values
        '''
        for k in kwargs.keys():
            if kwargs[k] == None:
                df = df.filter(F.col(k).isNull())
            else:
                if kwargs[k] is not list:
                    df = df.filter(F.col(k).contains(kwargs[k]))
                else:
                    df = df.filter(F.col(k).isin(kwargs[k]))
        return df

    #endregion
    ################################################################
    #
    #   UTILS SPECIFIC
    #
    ################################################################
    #region UTILS SPCEIFIC

    def get_patients_with_most_observations(self, 
    code:str=None,
    is_component:bool=False
    )->pyspark.sql.dataframe.DataFrame:
        code_column = 'code_coding_code'
        value_column = 'valueQuantity_value'
        if is_component:
            code_column = 'component_' + code_column
            value_column = 'component_' + value_column
        df = self.observations.select("effectiveDateTime","id","valueQuantity_value","component_valueQuantity_value",
            "subject_reference","code_coding_code","component_code_coding_code")
        if code:
            df = df.filter(df[code_column] == code)
        df = df.withColumn('Patient_id',F.expr("substring(subject_reference, 9, length(subject_reference))"))
        df1 = df.groupBy('Patient_id').count()
        df1 = df1.orderBy(F.col("count").desc())
        return df1

    def get_codes_with_most_observations(self,
    patient_id:str=None,
    is_component:bool=False
    )->pyspark.sql.dataframe.DataFrame:
        code_column = 'code_coding_code'
        value_column = 'valueQuantity_value'
        if is_component:
            code_column = 'component_' + code_column
            value_column = 'component_' + value_column
        df = self.observations.select(
            "subject_reference",
            "code_coding_code",
            "component_code_coding_code"
            )
        df = df.withColumn('Patient_id',F.expr("substring(subject_reference, 9, length(subject_reference))"))
        if patient_id:
            df = df.filter(df['Patient_id'] == patient_id)
        df1 = df.groupBy(code_column).count()
        df1 = df1.orderBy(F.col("count").desc())
        return df1

    def get_patient_and_code_with_most_observation(self, 
        patients:list, codes:list, is_component=False, first_n=1):
        code_column = 'code_coding_code'
        if is_component:
            code_column = 'component_' + code_column
        df = self.observations
        df = df.withColumn('Patient_id',F.expr("substring(subject_reference, 9, length(subject_reference))"))
        df = df.filter(df['Patient_id'].isin(patients))
        df = df.filter(df[code_column].isin(codes))
        df1 = df.groupBy('Patient_id', code_column).count()
        df1 = df1.orderBy(F.col("count").desc())
        return df1.head(first_n)

    def get_patients_by_questionnaire(self, linkId:str)->pyspark.sql.dataframe.DataFrame:
        '''
        - linkId: es balance_disorders_tab_status

        Returns a DataFrame
        - subject_reference
        - count
        '''
        df = self.questionnaire
        df = df.select("subject_reference","item_linkId","item_answer_valueBoolean")
        df = df.filter(df['item_linkId'].contains(linkId))
        df = df.filter(df['item_answer_valueBoolean'] == True)
        df = df.withColumn('Patient_id',F.expr("substring(subject_reference, 9, length(subject_reference))"))
        df1 = df.select('Patient_id').distinct()

        return df1

    def get_observations_codes_by_name(self, name:str, is_component:bool=False, show_name:bool=False)->list:
        code_column = 'code_coding_code'
        display_column = 'code_coding_display'
        if is_component:
            code_column = 'component_' + code_column
            display_column = 'component_' + display_column
        df = self.observations.select("meta_profile","code_coding_code","component_code_coding_code","code_coding_display","component_code_coding_display")
        df = df.filter(df['meta_profile'].contains(name))
        if not show_name:
            df = df.groupBy(code_column).count()
        else:
            return df.groupBy(code_column,display_column).count().collect()
        return list(df.toPandas()[code_column])

    def get_observations_name_by_code(self, code:str, is_component:bool=False)->list:
        code_column = 'code_coding_code'
        if is_component:
            code_column = 'component_' + code_column
        df = self.observations.select("meta_profile","code_coding_code","component_code_coding_code",
            "code_coding_display","component_code_coding_display")
        df = df.filter(df[code_column].contains(code))
        df = df.groupBy("meta_profile","code_coding_display","component_code_coding_display").count()
        return df.collect()

    def select_observations(self):
        df = self.observations
        df = df.select(
            self.SUBJECT,
            self.TIME,
            self.CODE,
            self.CMP_CODE,
            self.VALUE,
            self.CMP_VALUE
        )
        return df

    def filter_observations(self, patients, codes, comp_codes):
        '''
        Shortcut
        '''
        df = self.select_observations()
        if patients:
            if patients is not list:
                if '/' not in patients:
                    patients = ('Patient/' + str(patients))
            else:
                new_p = []
                for p in patients:
                    if '/' not in p:
                        new_p.append('Patient/' + str(p))
                patients = new_p
        # df = self.filter(df, 'subject_reference', patients)
        # df = self.filter(df, 'code_coding_code', codes)
        # df = self.filter(df, 'component_code_coding_code', comp_codes)
        df = self.filter(df, subject_reference=patients,
            code_coding_code=codes, component_code_coding_code=comp_codes)
        return df

    def has_missing_values_observations(self, patient, code, comp_code):
        df = self.filter_observations(patient, code, comp_code)
        if code != None:
            if comp_code == None:
                fltr = ((F.col(self.CODE).isNotNull()) & 
                        (F.col(self.CMP_CODE).isNull()) &
                        (F.col(self.VALUE).isNull()))
            else:
                fltr = ((F.col(self.CODE).isNotNull()) & 
                        (F.col(self.CMP_CODE).isNotNull()) &
                        (F.col(self.CMP_VALUE).isNull()))
        else:
            fltr = ((F.col(self.CODE).isNull()) & 
                        (F.col(self.CMP_CODE).isNotNull()) &
                        (F.col(self.CMP_VALUE).isNull()))
        tmp = df.filter(fltr)
        return tmp.count() > 0, df.count(), tmp.count()

    def bruteforce_find_missing_observations(self):
        relations = self.get_alls(self.observations, [self.SUBJECT,self.CODE,self.CMP_CODE])
        tmp = relations
        print(tmp.count())
        ps = self.df_column_to_list(tmp, self.SUBJECT)
        cs = self.df_column_to_list(tmp, self.CODE)
        ms = self.df_column_to_list(tmp, self.CMP_CODE)
        print(len(ps),len(cs),len(ms))
        msng = []
        for i in range(0, tmp.count()):
            res, cnt1, cnt2 = self.has_missing_values_observations(ps[i],cs[i],ms[i])
            if res:
                msng.append((ps[i],cs[i],ms[i], cnt1, cnt2))
                print(ps[i],cs[i],ms[i], cnt1, cnt2)
        return

    def has_missing_time_values_observations(self, patient, code, comp_code, time='day'):
        '''
        - time: hour, day, week

        Returns true if has one and the list of missing ones
        '''
        assert time in ['hour','day','week']
        df = self.filter_observations(patient, code, comp_code)
        df = df.orderBy(F.col(self.TIME).asc())
        df = df.withColumn(self.LOCAL_TIME, F.date_trunc(time, self.TIME))
        tm = self.df_column_to_list(df, self.LOCAL_TIME)
        msng = []
        if time == 'hours':
            plus = datetime.timedelta(hours=1)
        elif time == 'day':
            plus = datetime.timedelta(days=1)
        elif time == 'week':
            plus = datetime.timedelta(weeks=1)
        prev = tm[0]
        for t in tm:
            dif = t - prev
            if dif > plus:
                # print('missing', dif, prev, t)
                fix = prev
                while (t - fix) != plus:
                    fix += plus
                    msng.append(fix)
                    # print('added', fix)
                prev += dif
            elif dif == plus:
                prev += dif
        return len(msng) > 0, msng

    def prepare_fill_missing_time_values_observations(self, patient, code, comp_code, 
        time='day'):
        '''
        - time: hour, day, week
        '''
        assert time in ['hour','day','week']
        df = self.filter_observations(patient, code, comp_code)
        # df = df.orderBy(F.col(self.TIME).asc())
        df = df.withColumn(self.LOCAL_TIME, F.date_trunc(time, self.TIME))
        df = df.groupBy(self.SUBJECT, self.LOCAL_TIME, self.CODE, self.CMP_CODE).agg(F.avg(self.VALUE),F.avg(self.CMP_VALUE))
        df = df.orderBy(F.col(self.LOCAL_TIME).asc())
        tm = self.df_column_to_list(df, self.LOCAL_TIME)
        new_patient = 'Patient/'+patient if '/' not in patient else patient
        empty_row = [new_patient, None, code, comp_code, None, None]
        new_schema = df.schema
        for f in new_schema.fields:
            f.nullable = True
        df = self.spark.createDataFrame(df.collect(), schema=new_schema)
        if time == 'hours':
            plus = datetime.timedelta(hours=1)
        elif time == 'day':
            plus = datetime.timedelta(days=1)
        elif time == 'week':
            plus = datetime.timedelta(weeks=1)
        prev = tm[0]
        for t in tm:
            dif = t - prev
            if dif > plus:
                # print('missing', dif, prev, t)
                fix = prev
                while (t - fix) != plus:
                    fix += plus
                    row_to_add = empty_row.copy()
                    row_to_add[1] = datetime.datetime(fix.year, fix.month, fix.day, fix.hour, fix.minute, fix.second)
                    newRow = self.spark.createDataFrame([row_to_add], schema=new_schema)
                    df = df.union(newRow)
                    # df = df.append(newRow, ignore_index=True)
                    # print('added', fix)
                prev += dif
            elif dif == plus:
                prev += dif
        df = df.orderBy(F.col(self.LOCAL_TIME).asc())
        return df

    #endregion
    ################################################################
    #
    #   EXPERIMENTS
    #
    ################################################################
    #region EXPERIMENTS

    def get_observations(self, codes:list=[], component_codes:list=[], patients:list=[], 
        time:str='none',fix_time=True, expr:str='none',expr_value=None, custom_expr=None)->pyspark.sql.dataframe.DataFrame:
        '''
        - codes: for filtering code_coding_code
        - component_codes: for filtering component_code_coding_codes
        - patients: for filtering by subject_reference
        - time: none, hour, day, week, month, quarter, year
        - expr: none, avg, quantile, mean, count, std, has_weekly
        - expr_value: es. for quantile [0.25] or [0.25, 0.75]
        - custom_expr

        Returns DataFrame
        - date
        - Patient_id
        - expr: the kind of operation we want to apply
        '''
        df = self.observations.select("effectiveDateTime",
            "id","subject_reference",
            "code_coding_code","component_code_coding_code",
            "component_valueQuantity_value","valueQuantity_value")
        df1 = df
        # filter codes
        if len(codes) > 0:
            df1 = df1.filter(df1['code_coding_code'].isin(codes))
        if len(component_codes) > 0:
            df1 = df1.filter(df1['component_code_coding_code'].isin(component_codes))
        # filter patients
        df2 = df1.withColumn('Patient_id',F.expr("substring(subject_reference, 9, length(subject_reference))"))
        if len(patients) > 0:
            df2 = df2.filter(df2['Patient_id'].isin(patients))
        print("Dataframe has " + str(df2.count()) + " rows, now fixing according to time")
        # fix time
        df3 = df2
        if time != 'none':
            df3 = df2.withColumn('unix_time', F.unix_timestamp(F.col('effectiveDateTime'), "yyyy-MM-dd"))
            df3 = df3.withColumn('local_ts', F.date_format('effectiveDateTime', 'yyyy/MM/dd'))
            df3 = df3.withColumn('local_date', F.date_trunc( time,'effectiveDateTime'))
        
        print("Dataframe has " + str(df3.count()) + " rows, now applying expressions...")
        # do the expression
        df4 = df3
        if expr != "none":
            if custom_expr == None:
                expr_col = 'component_valueQuantity_value' if len(component_codes) > 0 else 'valueQuantity_value'
                if expr == 'quantile':
                    exp = F.percentile_approx(F.col(expr_col), expr_value).alias(expr)
                if expr == 'avg':
                    exp = F.avg(F.col(expr_col)).alias(expr)
                if expr == 'mean':
                    exp = F.mean(F.col(expr_col)).alias(expr)
                if expr == 'count':
                    exp = F.count(F.col(expr_col)).alias(expr)
                if expr == 'std':
                    exp = F.stddev(F.col(expr_col)).alias(expr)
                if expr == 'has_weekly':
                    exp = F.count(F.col(expr_col)).alias(expr)

            df4 = df4.groupBy('local_date','Patient_id').agg(exp)
        # beautify
        df6 = df4
        if time != 'none':
            df5 = df4.orderBy(F.col("local_date").asc())
            df5 = df5.withColumn("ts",F.to_timestamp(F.col("local_date"))).withColumn("date",F.to_date(F.col("ts")))
            df5 = df5.select("date","Patient_id",expr) 
            # fill missing time
            df6 = df5
            if fix_time:
                null_row = { 
                    'date':datetime.date.today, 
                    'Patient_id':patients[0], 
                    expr:None 
                    }
                df6 = self.fill_missing_time(df5,null_row,time=time)
        # additional expr
        df7 = df6
        if expr == 'has_weekly':
            rows = df6.collect()
            new_rows = []
            expr_row_col = expr
            for k in rows[0].asDict().keys():
                if expr in k:
                    expr_row_col = k
            for row in rows:
                r = row.asDict()
                if r[expr_row_col] != None:
                    r[expr_row_col] = 'Yes'
                else:
                    r[expr_row_col] = 'No'
                new_rows.append(pyspark.sql.types.Row(**r))
            df7 = self.spark.createDataFrame(new_rows)
        return df7

    # def fill_missing_time(self, df:pyspark.sql.dataframe.DataFrame, null_row:dict, col_name:str='date', time='week')->pyspark.sql.dataframe.DataFrame:
    #     '''
    #     - df
    #     - col_name: colum where there's date
    #     - null_row:dict for creating null Row. Like
    #         {date:2021-12-6,Patient_id:1234,value:NULL}
    #     - time: es hour, day, week, month, quarter, year
    #     '''
    #     if time == 'week':
    #         df = self.fill_missing_week(df,null_row,col_name)
    #     elif time == 'day':
    #         df = self.fill_missing_day(df,null_row,col_name)
            
    #     return df

    # def fill_missing_day(self, df:pyspark.sql.dataframe.DataFrame, null_row:dict, col_name:str='date')->pyspark.sql.dataframe.DataFrame:
    #     '''
    #     - df
    #     - col_name: colum where there's date
    #     - null_row:dict for creating null Row. Like
    #         {date:2021-12-6,Patient_id:1234,value:NULL}
    #     '''
    #     rows = df.collect()
    #     schema = df.schema
    #     today = datetime.date.today()
    #     new_rows = []
    #     day_count = rows[0][col_name]
    #     while day_count.isoformat() != today.isoformat():
    #         has_date = False
    #         for row in rows:
    #             row_date = row[col_name]
    #             if day_count.isoformat() == row_date.isoformat():
    #                 new_rows.append(row)
    #                 rows.remove(row)
    #                 has_date = True
    #                 continue
    #         if not has_date:
    #             null_row[col_name] = day_count
    #             new_rows.append(pyspark.sql.types.Row(**null_row))

    #         day_count = datetime.date.fromordinal(day_count.toordinal()+1)

    #     return self.spark.createDataFrame(new_rows)

    # def fill_missing_week(self, df:pyspark.sql.dataframe.DataFrame, null_row:dict, col_name:str='date')->pyspark.sql.dataframe.DataFrame:
    #     '''
    #     - df
    #     - col_name: colum where there's date
    #     - null_row:dict for creating null Row. Like
    #         {date:2021-12-6,Patient_id:1234,value:NULL}
    #     '''
    #     rows = df.collect()
    #     schema = df.schema
    #     # create all weeks from first row to today
    #     today = datetime.date.today()
    #     weeks = []
    #     for y in range(rows[0][col_name].year, today.year+1):
    #         last_week = today.isocalendar().week \
    #             if y==today.year \
    #             else datetime.date(y,12,28).isocalendar().week
    #         first_week = rows[0][col_name].isocalendar().week \
    #             if y == rows[0][col_name].year else 1
    #         for w in range(first_week, last_week+1):
    #             weeks.append(datetime.date.fromisocalendar(year=y,week=w,day=1))
    #     # check if weeks in row weeks
    #     new_rows = []
    #     for w in weeks:
    #         for row in rows:
    #             if w == row[col_name]:
    #                 new_rows.append(row)
    #                 continue
    #         if len(new_rows) > 0 and w == new_rows[-1][col_name]:
    #             continue
    #         null_row[col_name] = w
    #         new_rows.append(pyspark.sql.types.Row(**null_row))

    #     return self.spark.createDataFrame(new_rows)
    
    #endregion 