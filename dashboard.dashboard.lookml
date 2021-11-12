- dashboard: f4_overview
  title: F4 Overview
  layout: newspaper
  preferred_viewer: dashboards-next
  elements:
  - title: Category Right Now
    name: Category Right Now
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    type: single_value
    fields: [fhm_summary.measurement_timestamp_second, classification.category_type_5_viz]
    filters: {}
    sorts: [fhm_summary.measurement_timestamp_second desc]
    limit: 500
    column_limit: 50
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    defaults_version: 1
    series_types: {}
    hidden_fields: [fhm_summary.measurement_timestamp_second]
    listen:
      Subjectid: fhm_summary.subjectid
    row: 0
    col: 0
    width: 10
    height: 6
  - title: Trending
    name: Trending
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    type: single_value
    fields: [fhm_summary.measurement_timestamp_second, classification.trending_viz]
    filters: {}
    sorts: [fhm_summary.measurement_timestamp_second desc]
    limit: 5000
    column_limit: 50
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    defaults_version: 1
    series_types: {}
    hidden_fields: [fhm_summary.measurement_timestamp_second]
    listen:
      Subjectid: fhm_summary.subjectid
    row: 0
    col: 10
    width: 6
    height: 6
  - title: Validation
    name: Validation
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    type: looker_line
    fields: [fhm_summary.average_fhr, fhm_summary.average_ua, fhm_summary.average_us,
      fhm_summary_contractions.sum_contractions_now, fhm_summary_accelerations.sum_accelerations_now,
      fhm_summary_decelerations.sum_decelerations_now, fhm_summary_uterine_stimulation.sum_uterine_stimulations_now,
      fhm_summary.average_baseline_fhr, fhm_summary.average_variability_fhr, fhm_summary.measurement_timestamp_second]
    filters: {}
    sorts: [fhm_summary.measurement_timestamp_second]
    limit: 5000
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    show_null_points: true
    interpolation: linear
    y_axes: [{label: '', orientation: left, series: [{axisId: fhm_summary.average_fhr,
            id: fhm_summary.average_fhr, name: Fetal HR}, {axisId: fhm_summary.average_ua,
            id: fhm_summary.average_ua, name: Uterine Pressure}, {axisId: fhm_summary.average_us,
            id: fhm_summary.average_us, name: Uterine Stimulation}, {axisId: fhm_summary.average_baseline_fhr,
            id: fhm_summary.average_baseline_fhr, name: Average Baseline Fhr}], showLabels: true,
        showValues: true, unpinAxis: false, tickDensity: default, tickDensityCustom: 5,
        type: linear}, {label: '', orientation: left, series: [{axisId: fhm_summary.average_variability_fhr,
            id: fhm_summary.average_variability_fhr, name: Fetal HR - Variability}],
        showLabels: true, showValues: true, maxValue: 50, minValue: 0, unpinAxis: false,
        tickDensity: default, type: linear}, {label: !!null '', orientation: right,
        series: [{axisId: fhm_summary_contractions.sum_contractions_now, id: fhm_summary_contractions.sum_contractions_now,
            name: Sum Contractions Now}, {axisId: fhm_summary_accelerations.sum_accelerations_now,
            id: fhm_summary_accelerations.sum_accelerations_now, name: Sum Accelerations
              Now}, {axisId: fhm_summary_decelerations.sum_decelerations_now, id: fhm_summary_decelerations.sum_decelerations_now,
            name: Sum Decelerations Now}, {axisId: fhm_summary_uterine_stimulation.sum_uterine_stimulations_now,
            id: fhm_summary_uterine_stimulation.sum_uterine_stimulations_now, name: Sum
              Uterine Stimulations Now}], showLabels: true, showValues: true, minValue: !!null '',
        unpinAxis: false, tickDensity: default, tickDensityCustom: 5, type: linear}]
    hidden_series: [fhm_summary.average_baseline_fhr, fhm_summary.average_variability_fhr]
    series_types:
      fhm_summary_contractions.sum_contractions_now: area
      fhm_summary_accelerations.sum_accelerations_now: area
      fhm_summary_decelerations.sum_decelerations_now: area
      fhm_summary_uterine_stimulation.sum_uterine_stimulations_now: area
    series_colors:
      fhm_summary_contractions.sum_contractions_now: "#ff9485"
      fhm_summary_accelerations.sum_accelerations_now: "#91cc78"
      fhm_summary_decelerations.sum_decelerations_now: "#93a9ff"
      fhm_summary_uterine_stimulation.sum_uterine_stimulations_now: "#857b76"
    defaults_version: 1
    hidden_fields:
    listen:
      Subjectid: fhm_summary.subjectid
    row: 22
    col: 0
    width: 24
    height: 11
  - title: 5-Tier Classification over Time
    name: 5-Tier Classification over Time
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    type: looker_column
    fields: [fhm_summary.measurement_timestamp_second, fhm_summary.count, classification.category_type_5]
    pivots: [classification.category_type_5]
    fill_fields: [fhm_summary.measurement_timestamp_second]
    filters: {}
    sorts: [fhm_summary.measurement_timestamp_second desc]
    limit: 5000
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    color_application:
      collection_id: 5-color-palette
      palette_id: 5-color-palette-categorical-0
      options:
        steps: 5
    series_types: {}
    series_colors:
      Category 1 - fhm_summary.count: green
      Category 3 - fhm_summary.count: red
      Category 2 - fhm_summary.count: "#FADF63"
    show_null_points: true
    interpolation: linear
    defaults_version: 1
    listen:
      Subjectid: fhm_summary.subjectid
    row: 6
    col: 0
    width: 24
    height: 8
  - title: 3-Tier Classification over Time
    name: 3-Tier Classification over Time
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    type: looker_column
    fields: [fhm_summary.measurement_timestamp_second, classification.category_type,
      fhm_summary.count]
    pivots: [classification.category_type]
    filters: {}
    sorts: [fhm_summary.measurement_timestamp_second desc, classification.category_type
        0]
    limit: 5000
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    color_application:
      collection_id: 5-color-palette
      palette_id: 5-color-palette-categorical-0
      options:
        steps: 5
    series_types: {}
    series_colors:
      Category 1 - fhm_summary.count: green
      Category 3 - fhm_summary.count: red
      Category 2 - fhm_summary.count: "#FADF63"
    show_null_points: true
    interpolation: linear
    defaults_version: 1
    listen:
      Subjectid: fhm_summary.subjectid
    row: 14
    col: 0
    width: 24
    height: 8
  - title: Description
    name: Description
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    type: looker_single_record
    fields: [fhm_summary.measurement_timestamp_second, classification.baseline_viz,
      classification.variability_viz, classification.accelerations_viz, classification.decelerations_viz]
    filters: {}
    sorts: [fhm_summary.measurement_timestamp_second desc]
    limit: 500
    column_limit: 50
    show_view_names: false
    series_types: {}
    defaults_version: 1
    listen:
      Subjectid: fhm_summary.subjectid
    row: 0
    col: 16
    width: 8
    height: 6
  filters:
  - name: Subjectid
    title: Subjectid
    type: field_filter
    default_value: Fake Patient A
    allow_multiple_values: true
    required: false
    ui_config:
      type: dropdown_menu
      display: popover
    model: f4_fetal_heartrate_monitoring
    explore: fhm_summary
    listens_to_filters: []
    field: fhm_summary.subjectid
