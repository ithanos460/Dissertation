Installation Guide:

---> Before start doing the below steps, it is worthwhile to mention that if the user just wants to check the visuals with the current data, the only steps needed are just the below:
	1) Download & Install Microsoft Power BI Desktop for Windows
	2) Download & open the "tool.pbix" file from the repo:
		https://github.com/ithanos460/Dissertation/blob/main/tool.pbix

---> If the user wants to replicate the whole procedure from scratch:
	1) Download & Install Python for Windows (the one used for the implemented system is version Python 3.8.5). Details about the python libraries' versions can be found inside the report under the appendices section.
		https://www.python.org/downloads/
	2) Download & Install postgresql for Windows (the one used for the implemented system is version 14.2)
		https://www.postgresql.org/download/windows/
	3) Download & Install pgAdmin for Windows (the one used for the implemented system is version 4.26)
		https://www.pgadmin.org/download/pgadmin-4-windows/
	4) Download & Install Microsoft Power BI Desktop for Windows (the one used for the implemented system is version 2.110.805.0 64-bit)
		https://powerbi.microsoft.com/en-us/downloads/
	5) Download the most recent data from the sources - specific links mentioned inside the report (the data used for the implemented system were stored in the repo, as well:
		zipped: https://github.com/ithanos460/Dissertation/blob/main/Data.zip
		unzipped: https://github.com/ithanos460/Dissertation/tree/main/Data).
	6) Run the codes with the below sequence:
		i. import_data.py
			1. Set the correct path for the var "path"
			2. Set the correct path for the var "txtfolder"
		ii. ETLs.py
		iii. star_schema.py
			1. Set the correct path for the var "path"
		iv. data_vault.py
			1. Set the correct path for the var "path"
		v. snowflake.py
			1. Set the correct path for the var "path"
		vi. denorm.py
	7) Import Data (or refresh if needed) inside PBI (if not already imported from the file "tool.pbix") and create the dashboards according to the details described inside the report.
