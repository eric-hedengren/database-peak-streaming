# FiberOpticsDatabasePeakStreaming
Python program that utilizes fiber optics designed to measure changes in tension on oil platform walls.

## Things to Customize in [Database Peak Streaming][1]
IP address of the Micro Optics machine
-instrument_ip = '10.0.0.55'

Number of unique peak frequencies
-num_of_peaks = 8

Number of skipped data points for long term storage
-lt_increment = 10 # Repeats | A minute

How often the loop breaks to write data
-write_data = 10 # Seconds | Every hour

Lifetime for short term storage 
-st_length = 30 # Seconds | A week

How long the program keeps running
-streaming_time = 101 # Seconds | Infinite

[1]: database_peak_streaming.py "Python File"
