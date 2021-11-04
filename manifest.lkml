project_name: "f4_fetal_heartrate_monitoring"

# # Use local_dependency: To enable referencing of another project
# # on this instance with include: statements
#
# local_dependency: {
#   project: "name_of_other_project"
# }

constant: connection_string {
  value: "{% if fhm_summary.database_choice._parameter_value == 'hack' %}hca_hack_poc{% elsif fhm_summary.database_choice._parameter_value == 'poc' %}gcp_hca_poc{% else %}hca_hack_poc{% endif %}"
}
