connection: "gcp_hca_poc"
# connection: "hca_hack_poc"
# connection: "@{connection_string}"

include: "/views/*.view.lkml"                # include all views in the views/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project
include: "dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

explore: fhm_summary {
  label: "(2) F4 - FHM - Summary Data (Advanced)"
  view_label: "**FHM Summary"
  join: decelerations {
    view_label: "Deceleration"
    sql: LEFT JOIN UNNEST(${fhm_summary.decelerations}) as decelerations ;;
    relationship: one_to_many
  }

  join: acceleration {
    view_label: "Acceleration"
    sql: LEFT JOIN UNNEST(${fhm_summary.acceleration}) as acceleration ;;
    relationship: one_to_many
  }

  join: measurements_last_60_min {
    view_label: "Measurements Last 60 Min"
    sql: LEFT JOIN UNNEST(${fhm_summary.measurements_last_60_min}) as measurements_last_60_min ;;
    relationship: one_to_many
  }

  join: uterine_stimulation {
    view_label: "Uterine Stimulation"
    sql: LEFT JOIN UNNEST(${fhm_summary.uterine_stimulation}) as uterine_stimulation ;;
    relationship: one_to_many
  }

  join: contractions {
    view_label: "Contraction"
    sql: LEFT JOIN UNNEST(${fhm_summary.contractions}) as contractions ;;
    relationship: one_to_many
  }

  join: fhm_summary_kpis {
    view_label: "FHM Summary KPIs (Summary)"
    sql_on: ${fhm_summary.pk} = ${fhm_summary_kpis.pk} ;;
    relationship: one_to_one
  }

  join: fhm_summary_decelerations {
    view_label: "Deceleration (Summary)"
    sql_on: ${fhm_summary.pk} = ${fhm_summary_decelerations.pk} ;;
    relationship: one_to_one
  }

  join: fhm_summary_accelerations {
    view_label: "Acceleration (Summary)"
    sql_on: ${fhm_summary.pk} = ${fhm_summary_accelerations.pk} ;;
    relationship: one_to_one
  }

  join: fhm_summary_measurements_last_60_min {
    view_label: "Measurements Last 60 Min (Summary)"
    sql_on: ${fhm_summary.pk} = ${fhm_summary_measurements_last_60_min.pk} ;;
    relationship: one_to_one
  }

  join: fhm_summary_uterine_stimulation {
    view_label: "Uterine Stimulation (Summary)"
    sql_on: ${fhm_summary.pk} = ${fhm_summary_uterine_stimulation.pk} ;;
    relationship: one_to_one
  }

  join: fhm_summary_contractions {
    view_label: "Contraction (Summary)"
    sql_on: ${fhm_summary.pk} = ${fhm_summary_contractions.pk} ;;
    relationship: one_to_one
  }

  join: classification {
    view_label: "*Classification"
    sql:  ;;
    relationship: one_to_one
  }
}

explore: fetal_heartrate_monitoring_sample {
  label: "(3) F4 - FHM - Raw Data"
}

explore: fetal_heartrate_monitoring_sample_pre {
  hidden: yes
}


############ Caching Logic ############

persist_with: new_data

### PDT Timeframes

datagroup: new_data {
  max_cache_age: "30 minutes"
  sql_trigger: SELECT max(measurement_timestamp) FROM `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table` ;;
}

datagroup: once_daily {
  max_cache_age: "24 hours"
  sql_trigger: SELECT current_date() ;;
}

datagroup: once_weekly {
  max_cache_age: "168 hours"
  sql_trigger: SELECT extract(week from current_date()) ;;
}

datagroup: once_monthly {
  max_cache_age: "720 hours"
  sql_trigger: SELECT extract(month from current_date()) ;;
}

datagroup: once_yearly {
  max_cache_age: "9000 hours"
  sql_trigger: SELECT extract(year from current_date()) ;;
}
