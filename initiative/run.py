import adform_weekly_report
import adobe_weekly_report
import facebook_weekly_report
import google_analytics_weekly_report
import transcom_weekly_report

# TODO fix adform requests logic
adform_weekly_report.main(scope="bmw")
adobe_weekly_report.main()
facebook_weekly_report.main()
google_analytics_weekly_report.main()
transcom_weekly_report.main()

