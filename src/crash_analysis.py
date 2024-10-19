from pyspark.sql import functions as F, Window
from pyspark.sql import Window
from src.visualization import save_visualization, save_visualization_multi_dim


def analyze_males_killed(primary_persons_df):
    # Filter for male individuals
    male_kills = primary_persons_df.filter(primary_persons_df['PRSN_GNDR_ID'] == 'MALE')
    # Group by crash ID and sum the death counts
    crashes_with_male_kills = male_kills.groupBy('CRASH_ID').agg(F.sum('DEATH_CNT').alias('total_deaths'))
    # Count crashes with total male deaths greater than 2
    count = crashes_with_male_kills.filter(crashes_with_male_kills['total_deaths'] > 2).count()
    return count

def analyze_two_wheelers(units_df):
    # Count two-wheeler vehicles (motorcycles)
    two_wheelers = units_df.filter(units_df['VEH_BODY_STYL_ID'].rlike('(?i)MOTORCYCLE'))
    two_wheelers_count = two_wheelers.count()
    return {'two_wheelers_count': two_wheelers_count}

def analyze_top_vehicle_makes(primary_persons_df, units_df):
    # Rename DEATH_CNT in both DataFrames to avoid conflicts
    primary_persons_df = primary_persons_df.withColumnRenamed('DEATH_CNT', 'primary_DEATH_CNT')
    units_df = units_df.withColumnRenamed('DEATH_CNT', 'unit_DEATH_CNT')
    merged_data = primary_persons_df.join(units_df, on='CRASH_ID', how='inner')
    merged_data = merged_data.withColumn('TOTAL_DEATH_CNT',
                                         F.col('primary_DEATH_CNT') + F.col('unit_DEATH_CNT'))
    filtered_data = merged_data.filter(
        (F.col('TOTAL_DEATH_CNT') > 0) &
        (F.col('PRSN_TYPE_ID') == 'DRIVER') &
        (F.col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED') &
        (F.col('VEH_MAKE_ID').isNotNull()) &
        (F.col('VEH_MAKE_ID') != 'NA') &
        (F.col('VEH_MAKE_ID') != '')
    )
    top_vehicle_makes = filtered_data.groupBy('VEH_MAKE_ID').count().orderBy(F.desc('count')).limit(5)
    save_visualization(top_vehicle_makes, x_col='VEH_MAKE_ID', y_col='count',
                       title='Top Vehicle Makes Where Driver Died and Airbags Did Not Deploy',
                       filename='output/visualizations/analysis_3_visual.png')
    return top_vehicle_makes

def analyze_valid_licences_hit_and_run(units_df, primary_person_df):
    # Count hit-and-run incidents involving valid driver licenses
    hit_and_run = units_df.filter(
        (units_df['VEH_HNR_FL'] == 'Y')
    )
    valid_licences = primary_person_df.filter(
        (primary_person_df['DRVR_LIC_TYPE_ID'].isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']))
    )
    count = hit_and_run.join(valid_licences, on='CRASH_ID', how='inner').count()
    return {'count': count}

def analyze_females_not_involved(primary_persons_df):
    # Identify the state with the highest number of crashes involving non-female drivers
    non_female_crashes = primary_persons_df.filter(primary_persons_df['PRSN_GNDR_ID'] != 'FEMALE')
    state_counts = non_female_crashes.groupBy('DRVR_LIC_STATE_ID').count()
    highest_state = state_counts.orderBy(F.desc('count')).first()['DRVR_LIC_STATE_ID']
    return {'highest_state': highest_state}

def analyze_top_veh_makes_for_injuries(units_df):
    # Analyze vehicle makes by total injuries and deaths
    total_injuries_and_deaths = units_df.groupBy('VEH_MAKE_ID').agg(
        F.sum('TOT_INJRY_CNT').alias('total_injuries'),
        F.sum('DEATH_CNT').alias('total_deaths')
    )
    total_injuries_and_deaths = total_injuries_and_deaths.withColumn(
        'TOTAL_COUNT', F.col('total_injuries') + F.col('total_deaths')
    )
    sorted_injuries_and_deaths = total_injuries_and_deaths.orderBy(F.desc('TOTAL_COUNT'))
    top_3_to_5 = sorted_injuries_and_deaths.limit(5).subtract(sorted_injuries_and_deaths.limit(2))
    save_visualization(
        top_3_to_5,
        x_col='VEH_MAKE_ID',
        y_col='TOTAL_COUNT',
        title='3rd to 5th Vehicle Makes by Injuries and Deaths',
        filename='output/visualizations/analysis_6_visual.png'
    )
    return top_3_to_5

def analyze_top_ethnic_group_per_body_style(primary_persons_df, units_df):
    # Identify top ethnic groups by vehicle body style
    merged_data_y = primary_persons_df.join(units_df, on='CRASH_ID', how='inner')
    merged_data_y = merged_data_y.filter(
        merged_data_y['VEH_BODY_STYL_ID'].isNotNull() &
        (merged_data_y['VEH_BODY_STYL_ID'] != '') &
        ~merged_data_y['VEH_BODY_STYL_ID'].isin(['NA','UNKNOWN', 'OTHER  (EXPLAIN IN NARRATIVE)', 'NOT REPORTED'])
    )
    ethnic_groups = merged_data_y.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count()
    window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(F.desc('count'))
    top_ethnic_groups = ethnic_groups.withColumn('rank', F.row_number().over(window)).filter(F.col('rank') == 1).drop('rank')
    save_visualization_multi_dim(top_ethnic_groups, title='Top Ethnic Groups by Body Style',
                                 filename='output/visualizations/analysis_7_visual.png')
    return top_ethnic_groups

def analyze_top_zip_codes_for_alcohol(primary_persons_df, units_df):
    # Analyze top zip codes for crashes involving alcohol
    merged_data = primary_persons_df.join(units_df, on='CRASH_ID')
    alcohol_crashes = merged_data.filter(
        (merged_data['CONTRIB_FACTR_1_ID'].rlike('ALCOHOL')) |
        (merged_data['CONTRIB_FACTR_2_ID'].rlike('ALCOHOL')) |
        (merged_data['CONTRIB_FACTR_P1_ID'].rlike('ALCOHOL'))
    )
    car_body_styles = ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR']
    car_crashes = alcohol_crashes.filter(alcohol_crashes['VEH_BODY_STYL_ID'].isin(car_body_styles))
    car_crashes = car_crashes.filter(car_crashes['DRVR_ZIP'].isNotNull() & (car_crashes['DRVR_ZIP'] != ''))
    top_zip_codes = car_crashes.groupBy('DRVR_ZIP').count().orderBy(F.desc('count')).limit(5)
    save_visualization(top_zip_codes, x_col='DRVR_ZIP', y_col='count',
                       title='Top 5 Zip Codes for Crashes Involving Alcohol',
                       filename='output/visualizations/analysis_8_visual.png')
    return top_zip_codes

def analyze_no_damages_high_damage_level(damages_df, units_df):
    # Count distinct crash IDs with no damage but high damage level
    no_damage_df = damages_df.filter(damages_df['DAMAGED_PROPERTY'].isin(["NONE", "NONE1"]))
    high_damage_df = units_df.filter(
        ((units_df['VEH_DMAG_SCL_1_ID'] > "DAMAGED 4") & (
            ~units_df['VEH_DMAG_SCL_1_ID'].isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) |
        ((units_df['VEH_DMAG_SCL_2_ID'] > "DAMAGED 4") & (
            ~units_df['VEH_DMAG_SCL_2_ID'].isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
    )
    merged_df = no_damage_df.join(high_damage_df, on="CRASH_ID")
    insured_vehicles = merged_df.filter(merged_df['FIN_RESP_TYPE_ID'] == "PROOF OF LIABILITY INSURANCE")
    distinct_crash_ids = insured_vehicles.select('CRASH_ID').distinct().count()
    return {'distinct_crash_ids': distinct_crash_ids}

def analyze_top_vehicle_makes_speeding(charges_df, primary_persons_df, units_df):
    # Analyze top vehicle makes involved in speeding incidents
    speeding_charges = charges_df.filter(charges_df['CHARGE'].rlike('SPEED'))
    top_25_states = (
        units_df.filter(units_df['VEH_LIC_STATE_ID'].isNotNull())
        .groupBy('VEH_LIC_STATE_ID')
        .count()
        .orderBy(F.desc('count'))
        .limit(25)
        .select('VEH_LIC_STATE_ID')
        .rdd.flatMap(lambda x: x).collect()  # Convert to list
    )
    top_10_colors = (
        units_df.filter(units_df['VEH_COLOR_ID'] != 'NA')
        .groupBy('VEH_COLOR_ID')
        .count()
        .orderBy(F.desc('count'))
        .limit(10)
        .select('VEH_COLOR_ID')
        .rdd.flatMap(lambda x: x).collect()  # Convert to list
    )
    filtered_data = (
        speeding_charges.join(primary_persons_df, on='CRASH_ID', how='inner')
        .join(units_df, on='CRASH_ID', how='inner')
        .filter(
            (primary_persons_df['DRVR_LIC_TYPE_ID'].isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])) &
            (units_df['VEH_COLOR_ID'].isin(top_10_colors)) &
            (units_df['VEH_LIC_STATE_ID'].isin(top_25_states))
        )
    )
    top_vehicle_makes = (
        filtered_data.groupBy('VEH_MAKE_ID')
        .count()
        .orderBy(F.desc('count'))
        .limit(5)
    )
    save_visualization(top_vehicle_makes, x_col='VEH_MAKE_ID', y_col='count',
                       title='analyze_top_vehicle_makes_speeding',
                       filename='output/visualizations/analysis_10_visual.png')
    return top_vehicle_makes
