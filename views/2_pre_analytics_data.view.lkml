##############################
### Part 1: Data in Table from SQL Script
##############################

view: fhm_summary {
  sql_table_name: `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table`
    ;;

######################
### Original Dimensions
######################

## PK
  dimension: pk {
    primary_key: yes
    type: string
    sql: ${subjectid} || ' | ' || ${measurement_timestamp_raw} ;;
  }

  dimension_group: measurement_timestamp {
    type: time
    timeframes: [
      raw,
      second,
      minute,
      minute3,
      minute5,
      minute15,
      minute30,
      time,
      date
    ]
    sql: ${TABLE}.measurement_timestamp ;;
  }

  dimension: subjectid {
    type: string
    sql: ${TABLE}.subjectid ;;
  }

### FHR
  dimension: fhr_value {
    type: string
    sql: ${TABLE}.fhr_value ;;
  }

  dimension: fhr_baseline {
    type: string
    sql: ${TABLE}.fhr_baseline ;;
  }

  dimension: fhr_variability {
    type: string
    sql: ${TABLE}.fhr_variability ;;
  }

  dimension: fhr_data_quality {
    type: string
    sql: ${TABLE}.fhr_data_quality ;;
  }

### UA
  dimension: ua_value {
    type: string
    sql: ${TABLE}.ua_value ;;
  }

  dimension: ua_baseline {
    type: string
    sql: ${TABLE}.ua_baseline ;;
  }

  dimension: ua_variability {
    type: string
    sql: ${TABLE}.ua_variability ;;
  }

  dimension: ua_data_quality {
    type: string
    sql: ${TABLE}.ua_data_quality ;;
  }

### US
  dimension: us_value {
    type: string
    sql: ${TABLE}.us_value ;;
  }

  dimension: us_baseline {
    type: string
    sql: ${TABLE}.us_baseline ;;
  }

  dimension: us_variability {
    type: string
    sql: ${TABLE}.us_variability ;;
  }

  dimension: us_data_quality {
    type: string
    sql: ${TABLE}.us_data_quality ;;
  }

### Views

  dimension: acceleration {
    hidden: yes
    sql: ${TABLE}.acceleration ;;
  }

  dimension: contractions {
    hidden: yes
    sql: ${TABLE}.contractions ;;
  }

  dimension: decelerations {
    hidden: yes
    sql: ${TABLE}.decelerations ;;
  }

  dimension: measurements_last_60_min {
    hidden: yes
    sql: ${TABLE}.measurements_last_60_min ;;
  }

  dimension: uterine_stimulation {
    hidden: yes
    sql: ${TABLE}.uterine_stimulation ;;
  }

######################
### Derived Dimensions
######################

  dimension: baseline_rounded {
    type: number
    sql: round(${fhr_baseline} / 5,0) * 5 ;;
  }

  dimension: is_baseline_bradycardia {
    type: yesno
    sql: ${fhr_baseline} < 110 ;;
  }

  dimension: is_baseline_tachycardia {
    type: yesno
    sql: ${fhr_baseline} > 160 ;;
  }

  dimension: variability_type {
    type: string
    sql:
      case
        when ${fhr_variability} < 5 then 'Minimal'
        when ${fhr_variability} < 25 then 'Moderate'
        when ${fhr_variability} > 25 then 'Marked'
        when ${fhr_variability} is null then 'Absent'
        else '5 - Unknown'
      end
    ;;
  }

######################
### Measures
######################

  measure: count {
    type: count
    drill_fields: []
  }

  measure: average_fhr {
    group_label: "Clinical Measurements"
    label: "Fetal HR"
    description: "Average Fetal Heart Rate (BPM) measured - striaght average of HR1, HR2; Goal: 110-160 BPM"
    type: average
    sql: ${fhr_value} ;;
    value_format_name: decimal_1
  }

  measure: average_ua {
    group_label: "Clinical Measurements"
    label: "Uterine Pressure"
    description: "Average Uterine Pressure"
    type: average
    sql: ${ua_value} ;;
    value_format_name: decimal_1
  }

  measure: average_us {
    group_label: "Clinical Measurements"
    label: "Uterine Stimulation"
    description: "Average Uterine Stimulation; note: this only exists in synthetic data"
    type: average
    sql: ${us_value} ;;
    value_format_name: decimal_1
  }

  measure: average_baseline_fhr {
    type: average
    sql: ${fhr_baseline} ;;
  }

  measure: average_variability_fhr {
    group_label: "Clinical Measurements"
    label: "Fetal HR - Variability"
    type: average
    sql: ${fhr_variability} ;;
    value_format_name: decimal_1
  }

}

view: measurements_last_60_min {

######################
### Original Dimensions
######################

  dimension: datatype {
    type: number
    sql: ${TABLE}.datatype ;;
  }

  dimension_group: measurement_timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.measurement_timestamp ;;
  }

  dimension: fhr_value {
    type: number
    sql: ${TABLE}.fhr_value ;;
  }

######################
### Derived Dimensions
######################

  dimension: is_last_20_minutes {
    type: yesno
    sql: timestamp_diff(${fhm_summary.measurement_timestamp_raw},${measurement_timestamp_raw}, minute) < 20 ;;
  }

  dimension: is_bradycardia_value {
    type: yesno
    sql: ${fhr_value} < 110 ;;
  }

  dimension: is_tachycardia_value {
    type: yesno
    sql: ${fhr_value} < 160 ;;
  }

######################
### Measures
######################

  measure: count_bradycardia_value {
    type: count
    filters: [is_last_20_minutes: "Yes", is_bradycardia_value: "Yes"]
  }

  measure: count_tachycardia_value {
    type: count
    filters: [is_last_20_minutes: "Yes", is_bradycardia_value: "Yes"]
  }

  measure: count_total_value {
    type: count
    filters: [is_last_20_minutes: "Yes"]
  }

  measure: percent_bradycardia_value {
    type: number
    sql: ${count_bradycardia_value} / nullif(${count_total_value},0) ;;
    value_format_name: percent_1
  }

  measure: percent_tachycardia_value {
    type: number
    sql: ${count_tachycardia_value} / nullif(${count_total_value},0) ;;
    value_format_name: percent_1
  }
}

view: decelerations {

######################
### Original Dimensions
######################
  dimension: deceleration_amplitude {
    type: number
    sql: ${TABLE}.deceleration_amplitude ;;
  }

  dimension_group: deceleration_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.deceleration_end_time ;;
  }

  dimension_group: deceleration_first_peak {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.deceleration_first_peak ;;
  }

  dimension: deceleration_length {
    type: number
    sql: ${TABLE}.deceleration_length ;;
  }

  dimension: deceleration_seconds_to_peak {
    type: number
    sql: ${TABLE}.deceleration_seconds_to_peak ;;
  }

  dimension_group: deceleration_start {
    type: time
    timeframes: [
      raw,
      second,
      minute,
      minute3,
      minute5,
      minute15,
      minute30,
      time,
      date
    ]
    sql: ${TABLE}.deceleration_start_time ;;
  }

  dimension: end_time_diff {
    type: number
    sql: ${TABLE}.end_time_diff ;;
  }

  dimension_group: matching_contraction_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.matching_contraction_end ;;
  }

  dimension: matching_contraction_length {
    type: number
    sql: ${TABLE}.matching_contraction_length ;;
  }

  dimension_group: matching_contraction_peak {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.matching_contraction_peak ;;
  }

  dimension_group: matching_contraction_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.matching_contraction_start ;;
  }

  dimension: peak_time_diff {
    type: number
    sql: ${TABLE}.peak_time_diff ;;
  }

  dimension: seconds_overlap {
    type: number
    sql: ${TABLE}.seconds_overlap ;;
  }

  dimension: start_time_diff {
    type: number
    sql: ${TABLE}.start_time_diff ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

######################
### Derived Dimensions
######################

  dimension: is_in_last_30_min {
    type: yesno
    sql: timestamp_diff(${fhm_summary.measurement_timestamp_raw},${deceleration_start_raw}, minute) < 30 ;;
  }

  dimension: episodic_vs_periodic {
    type: string
    sql: case when ${seconds_overlap} = 0 then 'Episodic' else 'Periodic' end ;;
  }

  dimension: gradual_vs_abrupt {
    type: string
    sql: case when ${deceleration_seconds_to_peak} < 30 then 'Abrupt' else 'Gradual' end ;;
  }

  dimension: deceleration_start_vs_contraction_start {
    type: string
    sql:
      case
        when abs(${start_time_diff}) < 5 then 'Same Time'
        when ${peak_time_diff} > 5 then 'Deceleration After Contraction'
      end ;;
  }

  dimension: deceleration_nadir_vs_contraction_peak {
    type: string
    sql:
      case
        when abs(${peak_time_diff}) < 5 then 'Same Time'
        when ${peak_time_diff} > 5 then 'Deceleration After Contraction'
      end ;;
  }

  dimension: is_early_deceleration {
    type: yesno
    sql:
          ${gradual_vs_abrupt} = 'Gradual'
      and ${deceleration_nadir_vs_contraction_peak} = 'Same Time' ;;
  }

  dimension: is_late_deceleration {
    type: yesno
    sql:
          ${gradual_vs_abrupt} = 'Gradual'
      and ${deceleration_nadir_vs_contraction_peak} = 'Deceleration After Contraction'
      and ${deceleration_start_vs_contraction_start} = 'Deceleration After Contraction' ;;
  }

  dimension: is_variable_deceleration {
    type: yesno
    sql:
          ${gradual_vs_abrupt} = 'Abrupt'
      and ${deceleration_length} BETWEEN 15 and 120 ;;
  }

  dimension: is_deceleration_now {
    type: yesno
    sql: ${fhm_summary.measurement_timestamp_raw} BETWEEN ${deceleration_start_raw} AND ${deceleration_end_raw} ;;
  }

######################
### Measures
######################

  measure: count_early_decelerations {
    type: count
    filters: [is_in_last_30_min: "Yes", is_early_deceleration: "Yes"]
  }

  measure: count_late_decelerations {
    type: count
    filters: [is_in_last_30_min: "Yes", is_late_deceleration: "Yes"]
  }

  measure: count_variable_decelerations {
    type: count
    filters: [is_in_last_30_min: "Yes", is_variable_deceleration: "Yes"]
  }

  measure: count_decelerations_now {
    type: count_distinct
    sql: ${deceleration_start_raw} ;;
    filters: [is_deceleration_now: "Yes"]
  }

}

view: acceleration {

######################
### Original Dimensions
######################
  dimension: acceleration_amplitude {
    type: number
    sql: ${TABLE}.acceleration_amplitude ;;
  }

  dimension_group: acceleration_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.acceleration_end_time ;;
  }

  dimension_group: acceleration_first_peak {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.acceleration_first_peak ;;
  }

  dimension: acceleration_length {
    type: number
    sql: ${TABLE}.acceleration_length ;;
  }

  dimension: acceleration_seconds_to_peak {
    type: number
    sql: ${TABLE}.acceleration_seconds_to_peak ;;
  }

  dimension_group: acceleration_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.acceleration_start_time ;;
  }

  dimension_group: prior_acc_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.prior_acc_end_time ;;
  }

  dimension: prior_acc_event_id {
    type: number
    sql: ${TABLE}.prior_acc_event_id ;;
  }

  dimension_group: prior_acc_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.prior_acc_start_time ;;
  }

######################
### Derived Dimensions
######################

  dimension: is_in_last_30_min {
    type: yesno
    sql: timestamp_diff(${fhm_summary.measurement_timestamp_raw},${acceleration_start_raw}, minute) < 30 ;;
  }

  dimension: abrupt_vs_gradual {
    type: yesno
    sql: case when ${acceleration_seconds_to_peak} < 30 then 'Abrupt' else 'Gradual' end ;;
  }

  dimension: quick_vs_prolonged {
    type: yesno
    sql: case when ${acceleration_length} < 120 then 'Quick' else 'Prolonged' end ;;
  }

  dimension: is_reactivity {
    type: yesno
    sql: timestamp_diff(${acceleration_start_raw},${prior_acc_start_raw}, minute) <= 20  ;;
  }

  dimension: is_acceleration_now {
    type: yesno
    sql: ${fhm_summary.measurement_timestamp_raw} BETWEEN ${acceleration_start_raw} AND ${acceleration_end_raw} ;;
  }

######################
### Measures
######################

  measure: count_abrupt_acceleration {
    type: count
    filters: [is_in_last_30_min: "Yes", abrupt_vs_gradual: "Abrupt"]
  }

  measure: count_prolonged_acceleration {
    type: count
    filters: [is_in_last_30_min: "Yes", quick_vs_prolonged: "Prolonged"]
  }

  measure: count_reactivity {
    type: count
    filters: [is_in_last_30_min: "Yes", is_reactivity: "Yes"]
  }

  measure: count_accelerations_now {
    type: count_distinct
    sql: ${acceleration_start_raw} ;;
    filters: [is_acceleration_now: "Yes"]
  }
}

view: uterine_stimulation {

######################
### Original Dimensions
######################

  dimension: end_time_diff {
    type: number
    sql: ${TABLE}.end_time_diff ;;
  }

  dimension_group: matching_acceleration_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.matching_acceleration_end ;;
  }

  dimension: matching_acceleration_length {
    type: number
    sql: ${TABLE}.matching_acceleration_length ;;
  }

  dimension_group: matching_acceleration_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.matching_acceleration_start ;;
  }

  dimension: seconds_overlap {
    type: number
    sql: ${TABLE}.seconds_overlap ;;
  }

  dimension: start_time_diff {
    type: number
    sql: ${TABLE}.start_time_diff ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: uterine_stimulation_amplitude {
    type: number
    sql: ${TABLE}.uterine_stimulation_amplitude ;;
  }

  dimension_group: uterine_stimulation_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.uterine_stimulation_end_time ;;
  }

  dimension: uterine_stimulation_length {
    type: number
    sql: ${TABLE}.uterine_stimulation_length ;;
  }

  dimension_group: uterine_stimulation_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.uterine_stimulation_start_time ;;
  }

######################
### Derived Dimensions
######################

  dimension: is_in_last_30_min {
    type: yesno
    sql: timestamp_diff(${fhm_summary.measurement_timestamp_raw},${uterine_stimulation_start_raw}, minute) < 30 ;;
  }

  dimension: is_not_responding_to_stim {
    type: yesno
    sql: ${seconds_overlap} = 0 ;;
  }

  dimension: is_uterine_stimulation_now {
    type: yesno
    sql: ${fhm_summary.measurement_timestamp_raw} BETWEEN ${uterine_stimulation_start_raw} AND ${uterine_stimulation_end_raw} ;;
  }

######################
### Measures
######################

  measure: count_not_responding_to_stim {
    type: count
    filters: [is_in_last_30_min: "Yes", is_not_responding_to_stim: "Yes"]
  }

  measure: count_uterine_stimulations_now {
    type: count_distinct
    sql: ${uterine_stimulation_start_raw} ;;
    filters: [is_uterine_stimulation_now: "Yes"]
  }
}

view: contractions {

######################
### Original Dimensions
######################

  dimension: contraction_amplitude {
    type: number
    sql: ${TABLE}.contraction_amplitude ;;
  }

  dimension_group: contraction_end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.contraction_end_time ;;
  }

  dimension_group: contraction_first_peak {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.contraction_first_peak ;;
  }

  dimension: contraction_length {
    type: number
    sql: ${TABLE}.contraction_length ;;
  }

  dimension: contraction_seconds_to_peak {
    type: number
    sql: ${TABLE}.contraction_seconds_to_peak ;;
  }

  dimension_group: contraction_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.contraction_start_time ;;
  }

######################
### Derived Dimensions
######################

  dimension: is_in_last_30_min {
    type: yesno
    sql: timestamp_diff(${fhm_summary.measurement_timestamp_raw},${contraction_start_raw}, minute) < 30 ;;
  }

  dimension: is_contraction_now {
    type: yesno
    sql: ${fhm_summary.measurement_timestamp_raw} BETWEEN ${contraction_start_raw} AND ${contraction_end_raw} ;;
  }

######################
### Measures
######################

  measure: count_contractions {
    type: count
    filters: [is_in_last_30_min: "Yes"]
  }

  measure: count_contractions_now {
    type: count_distinct
    sql: ${contraction_start_raw} ;;
    filters: [is_contraction_now: "Yes"]
  }

}

##############################
### Part 2: NDTs to Describe Every Action
##############################

view: fhm_summary_measurements_last_60_min {
  derived_table: {
    datagroup_trigger: new_data
    explore_source: fhm_summary {
      column: pk {}
      column: average_fhr {}
      column: average_us {}
      column: average_ua {}
      column: average_baseline_fhr {}
      column: average_variability_fhr {}
      column: percent_bradycardia_value { field: measurements_last_60_min.percent_bradycardia_value }
      column: percent_tachycardia_value { field: measurements_last_60_min.percent_tachycardia_value }
    }
  }
  dimension: pk {}
  dimension: average_fhr {
    type: number
  }
  dimension: average_us {
    type: number
  }
  dimension: average_ua {
    type: number
  }
  dimension: average_baseline_fhr {
    type: number
  }
  dimension: average_variability_fhr {
    type: number
  }
  dimension: percent_bradycardia_value {
    type: number
  }
  dimension: percent_tachycardia_value {
    type: number
  }
}

view: fhm_summary_decelerations {
  derived_table: {
    datagroup_trigger: new_data
    explore_source: fhm_summary {
      column: pk {}
      column: count_decelerations_now { field: decelerations.count_decelerations_now }
      column: count_early_decelerations { field: decelerations.count_early_decelerations }
      column: count_variable_decelerations { field: decelerations.count_variable_decelerations }
      column: count_late_decelerations { field: decelerations.count_late_decelerations }
    }
  }
  dimension: pk {}
  dimension: count_decelerations_now {
    type: number
  }
  dimension: count_early_decelerations {
    type: number
  }
  dimension: count_variable_decelerations {
    type: number
  }
  dimension: count_late_decelerations {
    type: number
  }

  measure: sum_decelerations_now {
    type: sum
    sql: ${count_decelerations_now} ;;
  }
}

view: fhm_summary_accelerations {
  derived_table: {
    datagroup_trigger: new_data
    explore_source: fhm_summary {
      column: pk {}
      column: count_accelerations_now { field: acceleration.count_accelerations_now }
      column: count_abrupt_acceleration { field: acceleration.count_abrupt_acceleration }
      column: count_prolonged_acceleration { field: acceleration.count_prolonged_acceleration }
      column: count_reactivity { field: acceleration.count_reactivity }
    }
  }
  dimension: pk {}
  dimension: count_accelerations_now {
    type: number
  }
  dimension: count_abrupt_acceleration {
    type: number
  }
  dimension: count_prolonged_acceleration {
    type: number
  }
  dimension: count_reactivity {
    type: number
  }

  measure: sum_accelerations_now {
    type: sum
    sql: ${count_accelerations_now} ;;
  }
}

view: fhm_summary_uterine_stimulation {
  derived_table: {
    datagroup_trigger: new_data
    explore_source: fhm_summary {
      column: pk {}
      column: count_uterine_stimulations_now { field: uterine_stimulation.count_uterine_stimulations_now }
      column: count_not_responding_to_stim { field: uterine_stimulation.count_not_responding_to_stim }
    }
  }
  dimension: pk {}
  dimension: count_uterine_stimulations_now {
    type: number
  }
  dimension: count_not_responding_to_stim {
    type: number
  }

  measure: sum_uterine_stimulations_now {
    type: sum
    sql: ${count_uterine_stimulations_now} ;;
  }
}

view: fhm_summary_contractions {
  derived_table: {
    datagroup_trigger: new_data
    explore_source: fhm_summary {
      column: pk {}
      column: count_contractions_now { field: contractions.count_contractions_now }
      column: count_contractions { field: contractions.count_contractions }
    }
  }
  dimension: pk {}
  dimension: count_contractions_now {
    type: number
  }
  dimension: count_contractions {
    type: number
  }

  measure: sum_contractions_now {
    type: sum
    sql: ${count_contractions_now} ;;
  }
}

##############################
### Part 3: Classification
##############################

view: classification {
  dimension: is_category_1 {
    type: yesno
    sql:
          ${fhm_summary_measurements_last_60_min.average_baseline_fhr} BETWEEN 110 and 160
      AND ${fhm_summary_measurements_last_60_min.percent_bradycardia_value} < 0.2
      AND ${fhm_summary_measurements_last_60_min.percent_tachycardia_value} < 0.2
      AND ${fhm_summary_measurements_last_60_min.average_variability_fhr} BETWEEN 6 and 25
      AND ${fhm_summary_uterine_stimulation.count_not_responding_to_stim} = 0
      AND ${fhm_summary_decelerations.count_variable_decelerations} = 0
      AND ${fhm_summary_decelerations.count_late_decelerations} = 0
    ;;
  }

  dimension: is_recurrent_late {
    type: yesno
    sql: ${fhm_summary_decelerations.count_late_decelerations} > 1  ;;
  }

  dimension: is_recurrent_variable {
    type: yesno
    sql: ${fhm_summary_decelerations.count_variable_decelerations} > 1  ;;
  }

  dimension: is_brady {
    type: yesno
    sql:
          ${fhm_summary_measurements_last_60_min.average_fhr} < 110
      OR  ${fhm_summary_measurements_last_60_min.percent_bradycardia_value} > 0.6
    ;;
  }

  dimension: is_category_3 {
    type: yesno
    sql:
          ${fhm_summary_measurements_last_60_min.average_variability_fhr} is NULL
      AND
      (
            ${is_recurrent_late}
        OR ${is_recurrent_variable}
        OR ${is_brady}
      )
    ;;
  }

  dimension: category_type {
    type: string
    sql:
      case
        when ${is_category_1} then 'Category 1'
        when ${is_category_3} then 'Category 3'
        else 'Category 2'
      end
    ;;
  }

  dimension: category_value {
    type: number
    sql: cast(right(${category_type},1) as int64) ;;
  }

  measure: max_category {
    type: max
    sql: ${category_value} ;;
    value_format_name: decimal_1
  }
}
