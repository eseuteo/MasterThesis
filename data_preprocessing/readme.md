## Guide on how to generate vital-signs time-series

There are several options for generating/downloading time-series for each patient

First, get_signals_missing_values_percentage needs to be run.
This will generate a csv containing the patient ids, altogether with the number of ICU stays, percentage of them that are missing, and if the required signals (HR, RESP, SPO2, ABPSYS, ABPMEAN, ABPDIAS) exist.
This file will be later used for extracting time series with that data.
The password for the user "mimicuser" needs to be set as an environment variable