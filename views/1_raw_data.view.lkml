view: fetal_heartrate_monitoring_sample {
  sql_table_name: `hca-data-sandbox.looker_scratch2.A3_f4_fetal_heartrate_monitoring_fetal_heartrate_monitoring_sample_pre` ;;

####################
### Original Columns
####################

  dimension_group: measurement {
    type: time
    timeframes: [
      raw,
      millisecond250,
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

  dimension: second10 {
    group_label: "Measurement Date"
    type: date_time
    sql: cast(${measurement_minute} || ':' || floor(extract(second from ${measurement_raw}) / 10)*10 as datetime) ;;
    #  ;;
  }

  dimension: datatype {
    type: string
    sql: ${TABLE}.datatype ;;
  }

  dimension: monitor_id {
    type: string
    sql: ${TABLE}.MonitorID ;;
  }

  dimension: sensor_type {
    description:
"External Fetal HR from Ultrasound
FECG    Fetal heart rate from fetal ECG obtained with a scalp electrode
Ext_MHR   Maternal heart rate from an external source (e.g. maternal SpO2 device)
MECG    Maternal ECG
TOCO    Uterine pressure from external sensor
IUP     Intrauterine pressure sensor cathode
INOP    Inoperable sensor (not receiving signal; attached to monitor but lying in the bed)
No_Trans  Nothing plugged in receptacle on monitor "
    type: string
    sql: ${TABLE}.SensorType ;;
  }

  dimension: subject_id {
    type: string
    sql: ${TABLE}.SubjectID ;;
  }

  dimension: value {
    type: string
    sql: ${TABLE}.value ;;
  }

####################
### Derived Columns
####################

####################
### Measures
####################

  measure: average_mhr {
    group_label: "Clinical Measurements"
    label: "Maternal HR"
    description: "Average Maternal Heart Rate (BPM) measured"
    type: average
    sql: ${value} ;;
    filters: [datatype: "MHR"]
    value_format_name: decimal_1
  }

  measure: average_fhr {
    group_label: "Clinical Measurements"
    label: "Fetal HR"
    description: "Average Fetal Heart Rate (BPM) measured - striaght average of HR1, HR2; Goal: 110-160 BPM"
    type: average
    sql: ${value} ;;
    filters: [datatype: "HR1,HR2"]
    value_format_name: decimal_1
  }

  measure: average_ua {
    group_label: "Clinical Measurements"
    label: "Uterine Pressure"
    description: "Average Uterine Pressure"
    type: average
    sql: ${value} ;;
    filters: [datatype: "UA"]
    value_format_name: decimal_1
  }

  measure: average_us {
    group_label: "Clinical Measurements"
    label: "Uterine Stimulation"
    description: "Average Uterine Stimulation; note: this only exists in synthetic data"
    type: average
    sql: ${value} ;;
    filters: [datatype: "US"]
    value_format_name: decimal_1
  }


  measure: inop_error_measurements {
    group_label: "Data Population Errors"
    label: "# Error - Inoperable Sensor"
    description: "# measurements (taken every 1/4 second) that the sensor was inoperable"
    type: count
    filters: [sensor_type: "INOP"]
  }

  measure: no_trans_error_measurements {
    group_label: "Data Population Errors"
    label: "# Error - No Transmission"
    description: "# measurements (taken every 1/4 second) that the transmission failed"
    type: count
    filters: [sensor_type: "No_Trans"]
  }

  measure: total_error_measurements {
    group_label: "Data Population Errors"
    label: "# Error - Total"
    description: "# measurements (taken every 1/4 second) that either sensor was inoperable or the transmission failed"
    type: number
    sql: ${inop_error_measurements} + ${no_trans_error_measurements} ;;
  }

  measure: total_measurements {
    label: "# Measurements - Total"
    description: "# measurements (taken every 1/4 second)"
    type: count
  }

  measure: inop_error_percent {
    group_label: "Data Population Errors"
    label: "% Error - Inoperable Sensor"
    description: "% measurements (taken every 1/4 second) that the sensor was inoperable"
    type: number
    sql: ${inop_error_measurements} / nullif(${total_measurements},0) ;;
    value_format_name: percent_1
  }

  measure: no_trans_error_percent {
    group_label: "Data Population Errors"
    label: "% Error - No Transmission"
    description: "% measurements (taken every 1/4 second) that the transmission failed"
    type: number
    sql: ${inop_error_measurements} / nullif(${total_measurements},0) ;;
    value_format_name: percent_1
  }

  measure: percent_error_measurements {
    group_label: "Data Population Errors"
    label: "% Error - Total"
    description: "% measurements (taken every 1/4 second) that either sensor was inoperable or the transmission failed"
    type: number
    sql: ${total_error_measurements} / nullif(${total_measurements},0) ;;
    value_format_name: percent_1
  }

}

view: fetal_heartrate_monitoring_sample_pre {
  derived_table: {
    publish_as_db_view: yes
    datagroup_trigger: once_yearly
    create_process: {
      sql_step:
        CREATE OR REPLACE TABLE ${SQL_TABLE_NAME} AS
        SELECT subjectid, cast(datetime as timestamp) as measurement_timestamp, datatype, MonitorID, sensortype, key, value
        FROM `hca-data-sandbox.fetal_heartrate.fetal_heartrate_monitoring`
        UNPIVOT(
          value for key in (
              data_value_1
            , data_value_2
            , data_value_3
            , data_value_4
            , data_value_5
            , data_value_6
            , data_value_7
            , data_value_8
            , data_value_9
            , data_value_10
            , data_value_11
            , data_value_12
            , data_value_13
            , data_value_14
            , data_value_15
            , data_value_16
            , data_value_17
            , data_value_18
            , data_value_19
            , data_value_20
            , data_value_21
            , data_value_22
            , data_value_23
            , data_value_24
            , data_value_25
            , data_value_26
            , data_value_27
            , data_value_28
            , data_value_29
            , data_value_30
            , data_value_31
            , data_value_32
            , data_value_33
            , data_value_34
            , data_value_35
            , data_value_36
            , data_value_37
            , data_value_38
            , data_value_39
            , data_value_40
            , data_value_41
            , data_value_42
            , data_value_43
            , data_value_44
            , data_value_45
            , data_value_46
            , data_value_47
            , data_value_48
            , data_value_49
            , data_value_50
            , data_value_51
            , data_value_52
            , data_value_53
            , data_value_54
            , data_value_55
            , data_value_56
            , data_value_57
            , data_value_58
            , data_value_59
            , data_value_60
            , data_value_61
            , data_value_62
            , data_value_63
            , data_value_64
            , data_value_65
            , data_value_66
            , data_value_67
            , data_value_68
            , data_value_69
            , data_value_70
            , data_value_71
            , data_value_72
            , data_value_73
            , data_value_74
            , data_value_75
            , data_value_76
            , data_value_77
            , data_value_78
            , data_value_79
            , data_value_80
            , data_value_81
            , data_value_82
            , data_value_83
            , data_value_84
            , data_value_85
            , data_value_86
            , data_value_87
            , data_value_88
            , data_value_89
            , data_value_90
            , data_value_91
            , data_value_92
            , data_value_93
            , data_value_94
            , data_value_95
            , data_value_96
            , data_value_97
            , data_value_98
            , data_value_99
            , data_value_100
            , data_value_101
            , data_value_102
            , data_value_103
            , data_value_104
            , data_value_105
            , data_value_106
            , data_value_107
            , data_value_108
            , data_value_109
            , data_value_110
            , data_value_111
            , data_value_112
            , data_value_113
            , data_value_114
            , data_value_115
            , data_value_116
            , data_value_117
            , data_value_118
            , data_value_119
            , data_value_120
            , data_value_121
            , data_value_122
            , data_value_123
            , data_value_124
            , data_value_125
            , data_value_126
            , data_value_127
            , data_value_128
            , data_value_129
            , data_value_130
            , data_value_131
            , data_value_132
            , data_value_133
            , data_value_134
            , data_value_135
            , data_value_136
            , data_value_137
            , data_value_138
            , data_value_139
            , data_value_140
            , data_value_141
            , data_value_142
            , data_value_143
            , data_value_144
            , data_value_145
            , data_value_146
            , data_value_147
            , data_value_148
            , data_value_149
            , data_value_150
            , data_value_151
            , data_value_152
            , data_value_153
            , data_value_154
            , data_value_155
            , data_value_156
            , data_value_157
            , data_value_158
            , data_value_159
            , data_value_160
            , data_value_161
            , data_value_162
            , data_value_163
            , data_value_164
            , data_value_165
            , data_value_166
            , data_value_167
            , data_value_168
            , data_value_169
            , data_value_170
            , data_value_171
            , data_value_172
            , data_value_173
            , data_value_174
            , data_value_175
            , data_value_176
            , data_value_177
            , data_value_178
            , data_value_179
            , data_value_180
            , data_value_181
            , data_value_182
            , data_value_183
            , data_value_184
            , data_value_185
            , data_value_186
            , data_value_187
            , data_value_188
            , data_value_189
            , data_value_190
            , data_value_191
            , data_value_192
            , data_value_193
            , data_value_194
            , data_value_195
            , data_value_196
            , data_value_197
            , data_value_198
            , data_value_199
            , data_value_200
            , data_value_201
            , data_value_202
            , data_value_203
            , data_value_204
            , data_value_205
            , data_value_206
            , data_value_207
            , data_value_208
            , data_value_209
            , data_value_210
            , data_value_211
            , data_value_212
            , data_value_213
            , data_value_214
            , data_value_215
            , data_value_216
            , data_value_217
            , data_value_218
            , data_value_219
            , data_value_220
            , data_value_221
            , data_value_222
            , data_value_223
            , data_value_224
            , data_value_225
            , data_value_226
            , data_value_227
            , data_value_228
            , data_value_229
            , data_value_230
            , data_value_231
            , data_value_232
            , data_value_233
            , data_value_234
            , data_value_235
            , data_value_236
            , data_value_237
            , data_value_238
            , data_value_239
            , data_value_240
          )
        )
      ;;
      sql_step:
        CREATE OR REPLACE TABLE ${SQL_TABLE_NAME} AS
        SELECT subjectid, measurement_timestamp, datatype, MonitorID, sensortype, key, value, cast(right(cast(key as string),length(cast(key as string))-length("data_value_")) as int64) as time_add_key
        FROM ${SQL_TABLE_NAME}
      ;;
      sql_step:
        CREATE OR REPLACE TABLE ${SQL_TABLE_NAME} AS
        SELECT subjectid, measurement_timestamp, datatype, MonitorID, sensortype, value, cast(floor((time_add_key-1)/4) as int64) as seconds_add, cast(mod(time_add_key-1,4)*25 as int64) as milliseconds_add
        FROM ${SQL_TABLE_NAME}
      ;;
      sql_step:
        CREATE OR REPLACE TABLE ${SQL_TABLE_NAME} AS
        SELECT subjectid, TIMESTAMP_ADD(TIMESTAMP_ADD(measurement_timestamp, INTERVAL seconds_add SECOND), INTERVAL milliseconds_add MILLISECOND) AS measurement_timestamp, datatype, MonitorID, sensortype, value
        FROM ${SQL_TABLE_NAME}
      ;;
      # sql_step:
      #   CREATE OR REPLACE TABLE ${SQL_TABLE_NAME} AS
      #   SELECT * FROM ${SQL_TABLE_NAME}
      #   UNION ALL
      #   -- See "sql_create_synthetic_patient.md" for script on generating one perfect patient
      #   SELECT * FROM `hca-data-sandbox.fetal_heartrate.synthetic_10_change_to_schema`
      # ;;
    }
  }
  dimension: subjectid {}
}


# ####################
# ### Choose Table
# ####################

#   parameter: choose_table {
#     type: unquoted
#     default_value: "real_sample_data_3_patients"
#     allowed_value: {
#       label: "Real Data - 3 Patients"
#       value: "real_sample_data_3_patients"
#     }
#     allowed_value: {
#       label: "Synthetic Data"
#       value: "synthetic_data"
#     }
#   }

  #   {% if choose_table._parameter_value == 'real_sample_data_3_patients' %} `hca-data-sandbox.looker_scratch2.A3_f4_fetal_heartrate_monitoring_fetal_heartrate_monitoring_sample_pre`
  #   {% elsif choose_table._parameter_value == 'synthetic_data' %} `hca-data-sandbox.fetal_heartrate.synthetic_10_change_to_schema`
  #   {% else %} `hca-data-sandbox.looker_scratch2.A3_f4_fetal_heartrate_monitoring_fetal_heartrate_monitoring_sample_pre`
  #   {% endif %}

  #   ;;
  # # `hca-data-sandbox.looker_scratch2.A3_f4_fetal_heartrate_monitoring_fetal_heartrate_monitoring_sample_pre` ;;
  # # `hca-data-sandbox.fetal_heartrate.synthetic_10_change_to_schema`
