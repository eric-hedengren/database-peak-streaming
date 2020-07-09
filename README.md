# FiberOpticsDatabasePeakStreaming
Python program that utilizes fiber optics designed to measure changes in tension on oil platform walls.

## Things to Customize in [Database Peak Streaming][1]
**IP address of the Micro Optics machine**

`instrument_ip = '10.0.0.55'`

**Number of unique peak frequencies**

`num_of_peaks = 120` _# Number of unique frequencies_

**Number of skipped data points for long term storage**

`lt_increment = 600` _# Repeats (10 per second) | A minute_

**How often the loop breaks to write data**

`write_data = 3600` _# Seconds | Every hour_

**Lifetime for short term storage**

`st_length = 604800` _# Seconds | A week_

**How long the program keeps running**

`streaming_time = 2**30` _# Seconds | As high as possible_

[1]: database_peak_streaming.py "Python File"
