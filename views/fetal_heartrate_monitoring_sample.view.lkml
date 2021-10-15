view: fetal_heartrate_monitoring_sample {
  sql_table_name:  ;;
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
    }
}
dimension: subjectid {}
}

#   sql_table_name: `hca-data-sandbox.fetal_heartrate_monitoring.felal_heartrate_monitoring_sample`
#     ;;

#   dimension: data_type {
#     type: string
#     sql: ${TABLE}.DataType ;;
#   }

#   dimension: data_value_1 {
#     type: number
#     sql: ${TABLE}.data_value_1 ;;
#   }

#   dimension: data_value_10 {
#     type: number
#     sql: ${TABLE}.data_value_10 ;;
#   }

#   dimension: data_value_100 {
#     type: number
#     sql: ${TABLE}.data_value_100 ;;
#   }

#   dimension: data_value_101 {
#     type: number
#     sql: ${TABLE}.data_value_101 ;;
#   }

#   dimension: data_value_102 {
#     type: number
#     sql: ${TABLE}.data_value_102 ;;
#   }

#   dimension: data_value_103 {
#     type: number
#     sql: ${TABLE}.data_value_103 ;;
#   }

#   dimension: data_value_104 {
#     type: number
#     sql: ${TABLE}.data_value_104 ;;
#   }

#   dimension: data_value_105 {
#     type: number
#     sql: ${TABLE}.data_value_105 ;;
#   }

#   dimension: data_value_106 {
#     type: number
#     sql: ${TABLE}.data_value_106 ;;
#   }

#   dimension: data_value_107 {
#     type: number
#     sql: ${TABLE}.data_value_107 ;;
#   }

#   dimension: data_value_108 {
#     type: number
#     sql: ${TABLE}.data_value_108 ;;
#   }

#   dimension: data_value_109 {
#     type: number
#     sql: ${TABLE}.data_value_109 ;;
#   }

#   dimension: data_value_11 {
#     type: number
#     sql: ${TABLE}.data_value_11 ;;
#   }

#   dimension: data_value_110 {
#     type: number
#     sql: ${TABLE}.data_value_110 ;;
#   }

#   dimension: data_value_111 {
#     type: number
#     sql: ${TABLE}.data_value_111 ;;
#   }

#   dimension: data_value_112 {
#     type: number
#     sql: ${TABLE}.data_value_112 ;;
#   }

#   dimension: data_value_113 {
#     type: number
#     sql: ${TABLE}.data_value_113 ;;
#   }

#   dimension: data_value_114 {
#     type: number
#     sql: ${TABLE}.data_value_114 ;;
#   }

#   dimension: data_value_115 {
#     type: number
#     sql: ${TABLE}.data_value_115 ;;
#   }

#   dimension: data_value_116 {
#     type: number
#     sql: ${TABLE}.data_value_116 ;;
#   }

#   dimension: data_value_117 {
#     type: number
#     sql: ${TABLE}.data_value_117 ;;
#   }

#   dimension: data_value_118 {
#     type: number
#     sql: ${TABLE}.data_value_118 ;;
#   }

#   dimension: data_value_119 {
#     type: number
#     sql: ${TABLE}.data_value_119 ;;
#   }

#   dimension: data_value_12 {
#     type: number
#     sql: ${TABLE}.data_value_12 ;;
#   }

#   dimension: data_value_120 {
#     type: number
#     sql: ${TABLE}.data_value_120 ;;
#   }

#   dimension: data_value_121 {
#     type: number
#     sql: ${TABLE}.data_value_121 ;;
#   }

#   dimension: data_value_122 {
#     type: number
#     sql: ${TABLE}.data_value_122 ;;
#   }

#   dimension: data_value_123 {
#     type: number
#     sql: ${TABLE}.data_value_123 ;;
#   }

#   dimension: data_value_124 {
#     type: number
#     sql: ${TABLE}.data_value_124 ;;
#   }

#   dimension: data_value_125 {
#     type: number
#     sql: ${TABLE}.data_value_125 ;;
#   }

#   dimension: data_value_126 {
#     type: number
#     sql: ${TABLE}.data_value_126 ;;
#   }

#   dimension: data_value_127 {
#     type: number
#     sql: ${TABLE}.data_value_127 ;;
#   }

#   dimension: data_value_128 {
#     type: number
#     sql: ${TABLE}.data_value_128 ;;
#   }

#   dimension: data_value_129 {
#     type: number
#     sql: ${TABLE}.data_value_129 ;;
#   }

#   dimension: data_value_13 {
#     type: number
#     sql: ${TABLE}.data_value_13 ;;
#   }

#   dimension: data_value_130 {
#     type: number
#     sql: ${TABLE}.data_value_130 ;;
#   }

#   dimension: data_value_131 {
#     type: number
#     sql: ${TABLE}.data_value_131 ;;
#   }

#   dimension: data_value_132 {
#     type: number
#     sql: ${TABLE}.data_value_132 ;;
#   }

#   dimension: data_value_133 {
#     type: number
#     sql: ${TABLE}.data_value_133 ;;
#   }

#   dimension: data_value_134 {
#     type: number
#     sql: ${TABLE}.data_value_134 ;;
#   }

#   dimension: data_value_135 {
#     type: number
#     sql: ${TABLE}.data_value_135 ;;
#   }

#   dimension: data_value_136 {
#     type: number
#     sql: ${TABLE}.data_value_136 ;;
#   }

#   dimension: data_value_137 {
#     type: number
#     sql: ${TABLE}.data_value_137 ;;
#   }

#   dimension: data_value_138 {
#     type: number
#     sql: ${TABLE}.data_value_138 ;;
#   }

#   dimension: data_value_139 {
#     type: number
#     sql: ${TABLE}.data_value_139 ;;
#   }

#   dimension: data_value_14 {
#     type: number
#     sql: ${TABLE}.data_value_14 ;;
#   }

#   dimension: data_value_140 {
#     type: number
#     sql: ${TABLE}.data_value_140 ;;
#   }

#   dimension: data_value_141 {
#     type: number
#     sql: ${TABLE}.data_value_141 ;;
#   }

#   dimension: data_value_142 {
#     type: number
#     sql: ${TABLE}.data_value_142 ;;
#   }

#   dimension: data_value_143 {
#     type: number
#     sql: ${TABLE}.data_value_143 ;;
#   }

#   dimension: data_value_144 {
#     type: number
#     sql: ${TABLE}.data_value_144 ;;
#   }

#   dimension: data_value_145 {
#     type: number
#     sql: ${TABLE}.data_value_145 ;;
#   }

#   dimension: data_value_146 {
#     type: number
#     sql: ${TABLE}.data_value_146 ;;
#   }

#   dimension: data_value_147 {
#     type: number
#     sql: ${TABLE}.data_value_147 ;;
#   }

#   dimension: data_value_148 {
#     type: number
#     sql: ${TABLE}.data_value_148 ;;
#   }

#   dimension: data_value_149 {
#     type: number
#     sql: ${TABLE}.data_value_149 ;;
#   }

#   dimension: data_value_15 {
#     type: number
#     sql: ${TABLE}.data_value_15 ;;
#   }

#   dimension: data_value_150 {
#     type: number
#     sql: ${TABLE}.data_value_150 ;;
#   }

#   dimension: data_value_151 {
#     type: number
#     sql: ${TABLE}.data_value_151 ;;
#   }

#   dimension: data_value_152 {
#     type: number
#     sql: ${TABLE}.data_value_152 ;;
#   }

#   dimension: data_value_153 {
#     type: number
#     sql: ${TABLE}.data_value_153 ;;
#   }

#   dimension: data_value_154 {
#     type: number
#     sql: ${TABLE}.data_value_154 ;;
#   }

#   dimension: data_value_155 {
#     type: number
#     sql: ${TABLE}.data_value_155 ;;
#   }

#   dimension: data_value_156 {
#     type: number
#     sql: ${TABLE}.data_value_156 ;;
#   }

#   dimension: data_value_157 {
#     type: number
#     sql: ${TABLE}.data_value_157 ;;
#   }

#   dimension: data_value_158 {
#     type: number
#     sql: ${TABLE}.data_value_158 ;;
#   }

#   dimension: data_value_159 {
#     type: number
#     sql: ${TABLE}.data_value_159 ;;
#   }

#   dimension: data_value_16 {
#     type: number
#     sql: ${TABLE}.data_value_16 ;;
#   }

#   dimension: data_value_160 {
#     type: number
#     sql: ${TABLE}.data_value_160 ;;
#   }

#   dimension: data_value_161 {
#     type: number
#     sql: ${TABLE}.data_value_161 ;;
#   }

#   dimension: data_value_162 {
#     type: number
#     sql: ${TABLE}.data_value_162 ;;
#   }

#   dimension: data_value_163 {
#     type: number
#     sql: ${TABLE}.data_value_163 ;;
#   }

#   dimension: data_value_164 {
#     type: number
#     sql: ${TABLE}.data_value_164 ;;
#   }

#   dimension: data_value_165 {
#     type: number
#     sql: ${TABLE}.data_value_165 ;;
#   }

#   dimension: data_value_166 {
#     type: number
#     sql: ${TABLE}.data_value_166 ;;
#   }

#   dimension: data_value_167 {
#     type: number
#     sql: ${TABLE}.data_value_167 ;;
#   }

#   dimension: data_value_168 {
#     type: number
#     sql: ${TABLE}.data_value_168 ;;
#   }

#   dimension: data_value_169 {
#     type: number
#     sql: ${TABLE}.data_value_169 ;;
#   }

#   dimension: data_value_17 {
#     type: number
#     sql: ${TABLE}.data_value_17 ;;
#   }

#   dimension: data_value_170 {
#     type: number
#     sql: ${TABLE}.data_value_170 ;;
#   }

#   dimension: data_value_171 {
#     type: number
#     sql: ${TABLE}.data_value_171 ;;
#   }

#   dimension: data_value_172 {
#     type: number
#     sql: ${TABLE}.data_value_172 ;;
#   }

#   dimension: data_value_173 {
#     type: number
#     sql: ${TABLE}.data_value_173 ;;
#   }

#   dimension: data_value_174 {
#     type: number
#     sql: ${TABLE}.data_value_174 ;;
#   }

#   dimension: data_value_175 {
#     type: number
#     sql: ${TABLE}.data_value_175 ;;
#   }

#   dimension: data_value_176 {
#     type: number
#     sql: ${TABLE}.data_value_176 ;;
#   }

#   dimension: data_value_177 {
#     type: number
#     sql: ${TABLE}.data_value_177 ;;
#   }

#   dimension: data_value_178 {
#     type: number
#     sql: ${TABLE}.data_value_178 ;;
#   }

#   dimension: data_value_179 {
#     type: number
#     sql: ${TABLE}.data_value_179 ;;
#   }

#   dimension: data_value_18 {
#     type: number
#     sql: ${TABLE}.data_value_18 ;;
#   }

#   dimension: data_value_180 {
#     type: number
#     sql: ${TABLE}.data_value_180 ;;
#   }

#   dimension: data_value_181 {
#     type: number
#     sql: ${TABLE}.data_value_181 ;;
#   }

#   dimension: data_value_182 {
#     type: number
#     sql: ${TABLE}.data_value_182 ;;
#   }

#   dimension: data_value_183 {
#     type: number
#     sql: ${TABLE}.data_value_183 ;;
#   }

#   dimension: data_value_184 {
#     type: number
#     sql: ${TABLE}.data_value_184 ;;
#   }

#   dimension: data_value_185 {
#     type: number
#     sql: ${TABLE}.data_value_185 ;;
#   }

#   dimension: data_value_186 {
#     type: number
#     sql: ${TABLE}.data_value_186 ;;
#   }

#   dimension: data_value_187 {
#     type: number
#     sql: ${TABLE}.data_value_187 ;;
#   }

#   dimension: data_value_188 {
#     type: number
#     sql: ${TABLE}.data_value_188 ;;
#   }

#   dimension: data_value_189 {
#     type: number
#     sql: ${TABLE}.data_value_189 ;;
#   }

#   dimension: data_value_19 {
#     type: number
#     sql: ${TABLE}.data_value_19 ;;
#   }

#   dimension: data_value_190 {
#     type: number
#     sql: ${TABLE}.data_value_190 ;;
#   }

#   dimension: data_value_191 {
#     type: number
#     sql: ${TABLE}.data_value_191 ;;
#   }

#   dimension: data_value_192 {
#     type: number
#     sql: ${TABLE}.data_value_192 ;;
#   }

#   dimension: data_value_193 {
#     type: number
#     sql: ${TABLE}.data_value_193 ;;
#   }

#   dimension: data_value_194 {
#     type: number
#     sql: ${TABLE}.data_value_194 ;;
#   }

#   dimension: data_value_195 {
#     type: number
#     sql: ${TABLE}.data_value_195 ;;
#   }

#   dimension: data_value_196 {
#     type: number
#     sql: ${TABLE}.data_value_196 ;;
#   }

#   dimension: data_value_197 {
#     type: number
#     sql: ${TABLE}.data_value_197 ;;
#   }

#   dimension: data_value_198 {
#     type: number
#     sql: ${TABLE}.data_value_198 ;;
#   }

#   dimension: data_value_199 {
#     type: number
#     sql: ${TABLE}.data_value_199 ;;
#   }

#   dimension: data_value_2 {
#     type: number
#     sql: ${TABLE}.data_value_2 ;;
#   }

#   dimension: data_value_20 {
#     type: number
#     sql: ${TABLE}.data_value_20 ;;
#   }

#   dimension: data_value_200 {
#     type: number
#     sql: ${TABLE}.data_value_200 ;;
#   }

#   dimension: data_value_201 {
#     type: number
#     sql: ${TABLE}.data_value_201 ;;
#   }

#   dimension: data_value_202 {
#     type: number
#     sql: ${TABLE}.data_value_202 ;;
#   }

#   dimension: data_value_203 {
#     type: number
#     sql: ${TABLE}.data_value_203 ;;
#   }

#   dimension: data_value_204 {
#     type: number
#     sql: ${TABLE}.data_value_204 ;;
#   }

#   dimension: data_value_205 {
#     type: number
#     sql: ${TABLE}.data_value_205 ;;
#   }

#   dimension: data_value_206 {
#     type: number
#     sql: ${TABLE}.data_value_206 ;;
#   }

#   dimension: data_value_207 {
#     type: number
#     sql: ${TABLE}.data_value_207 ;;
#   }

#   dimension: data_value_208 {
#     type: number
#     sql: ${TABLE}.data_value_208 ;;
#   }

#   dimension: data_value_209 {
#     type: number
#     sql: ${TABLE}.data_value_209 ;;
#   }

#   dimension: data_value_21 {
#     type: number
#     sql: ${TABLE}.data_value_21 ;;
#   }

#   dimension: data_value_210 {
#     type: number
#     sql: ${TABLE}.data_value_210 ;;
#   }

#   dimension: data_value_211 {
#     type: number
#     sql: ${TABLE}.data_value_211 ;;
#   }

#   dimension: data_value_212 {
#     type: number
#     sql: ${TABLE}.data_value_212 ;;
#   }

#   dimension: data_value_213 {
#     type: number
#     sql: ${TABLE}.data_value_213 ;;
#   }

#   dimension: data_value_214 {
#     type: number
#     sql: ${TABLE}.data_value_214 ;;
#   }

#   dimension: data_value_215 {
#     type: number
#     sql: ${TABLE}.data_value_215 ;;
#   }

#   dimension: data_value_216 {
#     type: number
#     sql: ${TABLE}.data_value_216 ;;
#   }

#   dimension: data_value_217 {
#     type: number
#     sql: ${TABLE}.data_value_217 ;;
#   }

#   dimension: data_value_218 {
#     type: number
#     sql: ${TABLE}.data_value_218 ;;
#   }

#   dimension: data_value_219 {
#     type: number
#     sql: ${TABLE}.data_value_219 ;;
#   }

#   dimension: data_value_22 {
#     type: number
#     sql: ${TABLE}.data_value_22 ;;
#   }

#   dimension: data_value_220 {
#     type: number
#     sql: ${TABLE}.data_value_220 ;;
#   }

#   dimension: data_value_221 {
#     type: number
#     sql: ${TABLE}.data_value_221 ;;
#   }

#   dimension: data_value_222 {
#     type: number
#     sql: ${TABLE}.data_value_222 ;;
#   }

#   dimension: data_value_223 {
#     type: number
#     sql: ${TABLE}.data_value_223 ;;
#   }

#   dimension: data_value_224 {
#     type: number
#     sql: ${TABLE}.data_value_224 ;;
#   }

#   dimension: data_value_225 {
#     type: number
#     sql: ${TABLE}.data_value_225 ;;
#   }

#   dimension: data_value_226 {
#     type: number
#     sql: ${TABLE}.data_value_226 ;;
#   }

#   dimension: data_value_227 {
#     type: number
#     sql: ${TABLE}.data_value_227 ;;
#   }

#   dimension: data_value_228 {
#     type: number
#     sql: ${TABLE}.data_value_228 ;;
#   }

#   dimension: data_value_229 {
#     type: number
#     sql: ${TABLE}.data_value_229 ;;
#   }

#   dimension: data_value_23 {
#     type: number
#     sql: ${TABLE}.data_value_23 ;;
#   }

#   dimension: data_value_230 {
#     type: number
#     sql: ${TABLE}.data_value_230 ;;
#   }

#   dimension: data_value_231 {
#     type: number
#     sql: ${TABLE}.data_value_231 ;;
#   }

#   dimension: data_value_232 {
#     type: number
#     sql: ${TABLE}.data_value_232 ;;
#   }

#   dimension: data_value_233 {
#     type: number
#     sql: ${TABLE}.data_value_233 ;;
#   }

#   dimension: data_value_234 {
#     type: number
#     sql: ${TABLE}.data_value_234 ;;
#   }

#   dimension: data_value_235 {
#     type: number
#     sql: ${TABLE}.data_value_235 ;;
#   }

#   dimension: data_value_236 {
#     type: number
#     sql: ${TABLE}.data_value_236 ;;
#   }

#   dimension: data_value_237 {
#     type: number
#     sql: ${TABLE}.data_value_237 ;;
#   }

#   dimension: data_value_238 {
#     type: number
#     sql: ${TABLE}.data_value_238 ;;
#   }

#   dimension: data_value_239 {
#     type: number
#     sql: ${TABLE}.data_value_239 ;;
#   }

#   dimension: data_value_24 {
#     type: number
#     sql: ${TABLE}.data_value_24 ;;
#   }

#   dimension: data_value_240 {
#     type: number
#     sql: ${TABLE}.data_value_240 ;;
#   }

#   dimension: data_value_25 {
#     type: number
#     sql: ${TABLE}.data_value_25 ;;
#   }

#   dimension: data_value_26 {
#     type: number
#     sql: ${TABLE}.data_value_26 ;;
#   }

#   dimension: data_value_27 {
#     type: number
#     sql: ${TABLE}.data_value_27 ;;
#   }

#   dimension: data_value_28 {
#     type: number
#     sql: ${TABLE}.data_value_28 ;;
#   }

#   dimension: data_value_29 {
#     type: number
#     sql: ${TABLE}.data_value_29 ;;
#   }

#   dimension: data_value_3 {
#     type: number
#     sql: ${TABLE}.data_value_3 ;;
#   }

#   dimension: data_value_30 {
#     type: number
#     sql: ${TABLE}.data_value_30 ;;
#   }

#   dimension: data_value_31 {
#     type: number
#     sql: ${TABLE}.data_value_31 ;;
#   }

#   dimension: data_value_32 {
#     type: number
#     sql: ${TABLE}.data_value_32 ;;
#   }

#   dimension: data_value_33 {
#     type: number
#     sql: ${TABLE}.data_value_33 ;;
#   }

#   dimension: data_value_34 {
#     type: number
#     sql: ${TABLE}.data_value_34 ;;
#   }

#   dimension: data_value_35 {
#     type: number
#     sql: ${TABLE}.data_value_35 ;;
#   }

#   dimension: data_value_36 {
#     type: number
#     sql: ${TABLE}.data_value_36 ;;
#   }

#   dimension: data_value_37 {
#     type: number
#     sql: ${TABLE}.data_value_37 ;;
#   }

#   dimension: data_value_38 {
#     type: number
#     sql: ${TABLE}.data_value_38 ;;
#   }

#   dimension: data_value_39 {
#     type: number
#     sql: ${TABLE}.data_value_39 ;;
#   }

#   dimension: data_value_4 {
#     type: number
#     sql: ${TABLE}.data_value_4 ;;
#   }

#   dimension: data_value_40 {
#     type: number
#     sql: ${TABLE}.data_value_40 ;;
#   }

#   dimension: data_value_41 {
#     type: number
#     sql: ${TABLE}.data_value_41 ;;
#   }

#   dimension: data_value_42 {
#     type: number
#     sql: ${TABLE}.data_value_42 ;;
#   }

#   dimension: data_value_43 {
#     type: number
#     sql: ${TABLE}.data_value_43 ;;
#   }

#   dimension: data_value_44 {
#     type: number
#     sql: ${TABLE}.data_value_44 ;;
#   }

#   dimension: data_value_45 {
#     type: number
#     sql: ${TABLE}.data_value_45 ;;
#   }

#   dimension: data_value_46 {
#     type: number
#     sql: ${TABLE}.data_value_46 ;;
#   }

#   dimension: data_value_47 {
#     type: number
#     sql: ${TABLE}.data_value_47 ;;
#   }

#   dimension: data_value_48 {
#     type: number
#     sql: ${TABLE}.data_value_48 ;;
#   }

#   dimension: data_value_49 {
#     type: number
#     sql: ${TABLE}.data_value_49 ;;
#   }

#   dimension: data_value_5 {
#     type: number
#     sql: ${TABLE}.data_value_5 ;;
#   }

#   dimension: data_value_50 {
#     type: number
#     sql: ${TABLE}.data_value_50 ;;
#   }

#   dimension: data_value_51 {
#     type: number
#     sql: ${TABLE}.data_value_51 ;;
#   }

#   dimension: data_value_52 {
#     type: number
#     sql: ${TABLE}.data_value_52 ;;
#   }

#   dimension: data_value_53 {
#     type: number
#     sql: ${TABLE}.data_value_53 ;;
#   }

#   dimension: data_value_54 {
#     type: number
#     sql: ${TABLE}.data_value_54 ;;
#   }

#   dimension: data_value_55 {
#     type: number
#     sql: ${TABLE}.data_value_55 ;;
#   }

#   dimension: data_value_56 {
#     type: number
#     sql: ${TABLE}.data_value_56 ;;
#   }

#   dimension: data_value_57 {
#     type: number
#     sql: ${TABLE}.data_value_57 ;;
#   }

#   dimension: data_value_58 {
#     type: number
#     sql: ${TABLE}.data_value_58 ;;
#   }

#   dimension: data_value_59 {
#     type: number
#     sql: ${TABLE}.data_value_59 ;;
#   }

#   dimension: data_value_6 {
#     type: number
#     sql: ${TABLE}.data_value_6 ;;
#   }

#   dimension: data_value_60 {
#     type: number
#     sql: ${TABLE}.data_value_60 ;;
#   }

#   dimension: data_value_61 {
#     type: number
#     sql: ${TABLE}.data_value_61 ;;
#   }

#   dimension: data_value_62 {
#     type: number
#     sql: ${TABLE}.data_value_62 ;;
#   }

#   dimension: data_value_63 {
#     type: number
#     sql: ${TABLE}.data_value_63 ;;
#   }

#   dimension: data_value_64 {
#     type: number
#     sql: ${TABLE}.data_value_64 ;;
#   }

#   dimension: data_value_65 {
#     type: number
#     sql: ${TABLE}.data_value_65 ;;
#   }

#   dimension: data_value_66 {
#     type: number
#     sql: ${TABLE}.data_value_66 ;;
#   }

#   dimension: data_value_67 {
#     type: number
#     sql: ${TABLE}.data_value_67 ;;
#   }

#   dimension: data_value_68 {
#     type: number
#     sql: ${TABLE}.data_value_68 ;;
#   }

#   dimension: data_value_69 {
#     type: number
#     sql: ${TABLE}.data_value_69 ;;
#   }

#   dimension: data_value_7 {
#     type: number
#     sql: ${TABLE}.data_value_7 ;;
#   }

#   dimension: data_value_70 {
#     type: number
#     sql: ${TABLE}.data_value_70 ;;
#   }

#   dimension: data_value_71 {
#     type: number
#     sql: ${TABLE}.data_value_71 ;;
#   }

#   dimension: data_value_72 {
#     type: number
#     sql: ${TABLE}.data_value_72 ;;
#   }

#   dimension: data_value_73 {
#     type: number
#     sql: ${TABLE}.data_value_73 ;;
#   }

#   dimension: data_value_74 {
#     type: number
#     sql: ${TABLE}.data_value_74 ;;
#   }

#   dimension: data_value_75 {
#     type: number
#     sql: ${TABLE}.data_value_75 ;;
#   }

#   dimension: data_value_76 {
#     type: number
#     sql: ${TABLE}.data_value_76 ;;
#   }

#   dimension: data_value_77 {
#     type: number
#     sql: ${TABLE}.data_value_77 ;;
#   }

#   dimension: data_value_78 {
#     type: number
#     sql: ${TABLE}.data_value_78 ;;
#   }

#   dimension: data_value_79 {
#     type: number
#     sql: ${TABLE}.data_value_79 ;;
#   }

#   dimension: data_value_8 {
#     type: number
#     sql: ${TABLE}.data_value_8 ;;
#   }

#   dimension: data_value_80 {
#     type: number
#     sql: ${TABLE}.data_value_80 ;;
#   }

#   dimension: data_value_81 {
#     type: number
#     sql: ${TABLE}.data_value_81 ;;
#   }

#   dimension: data_value_82 {
#     type: number
#     sql: ${TABLE}.data_value_82 ;;
#   }

#   dimension: data_value_83 {
#     type: number
#     sql: ${TABLE}.data_value_83 ;;
#   }

#   dimension: data_value_84 {
#     type: number
#     sql: ${TABLE}.data_value_84 ;;
#   }

#   dimension: data_value_85 {
#     type: number
#     sql: ${TABLE}.data_value_85 ;;
#   }

#   dimension: data_value_86 {
#     type: number
#     sql: ${TABLE}.data_value_86 ;;
#   }

#   dimension: data_value_87 {
#     type: number
#     sql: ${TABLE}.data_value_87 ;;
#   }

#   dimension: data_value_88 {
#     type: number
#     sql: ${TABLE}.data_value_88 ;;
#   }

#   dimension: data_value_89 {
#     type: number
#     sql: ${TABLE}.data_value_89 ;;
#   }

#   dimension: data_value_9 {
#     type: number
#     sql: ${TABLE}.data_value_9 ;;
#   }

#   dimension: data_value_90 {
#     type: number
#     sql: ${TABLE}.data_value_90 ;;
#   }

#   dimension: data_value_91 {
#     type: number
#     sql: ${TABLE}.data_value_91 ;;
#   }

#   dimension: data_value_92 {
#     type: number
#     sql: ${TABLE}.data_value_92 ;;
#   }

#   dimension: data_value_93 {
#     type: number
#     sql: ${TABLE}.data_value_93 ;;
#   }

#   dimension: data_value_94 {
#     type: number
#     sql: ${TABLE}.data_value_94 ;;
#   }

#   dimension: data_value_95 {
#     type: number
#     sql: ${TABLE}.data_value_95 ;;
#   }

#   dimension: data_value_96 {
#     type: number
#     sql: ${TABLE}.data_value_96 ;;
#   }

#   dimension: data_value_97 {
#     type: number
#     sql: ${TABLE}.data_value_97 ;;
#   }

#   dimension: data_value_98 {
#     type: number
#     sql: ${TABLE}.data_value_98 ;;
#   }

#   dimension: data_value_99 {
#     type: number
#     sql: ${TABLE}.data_value_99 ;;
#   }

#   dimension_group: datetime {
#     type: time
#     timeframes: [
#       raw,
#       time,
#       date,
#       week,
#       month,
#       quarter,
#       year
#     ]
#     sql: ${TABLE}.Datetime ;;
#   }

#   dimension: monitor_id {
#     type: string
#     sql: ${TABLE}.MonitorID ;;
#   }

#   dimension: sensor_type {
#     type: string
#     sql: ${TABLE}.SensorType ;;
#   }

#   dimension: subject_id {
#     type: string
#     sql: ${TABLE}.SubjectID ;;
#   }

#   measure: count {
#     type: count
#     drill_fields: []
#   }
# }
