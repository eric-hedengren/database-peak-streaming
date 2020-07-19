itp = 'manually measured' # initial temperature | constant after measured | what temperature is measured
gf = .807 # gage factor at 22 C | constant after calculated from itp | room temperature?
metal_constant = 'unknown value' # metal constant | constant | specific to the type of metal sensors are measuring
alpha = 'unknown variable' # unsure what this variable provides
wl = None # wavelength | current wavelength data
iwl = None # initial wavelength | get data first row

total_strain = (10**6)*((wl-iwl)/iwl/gf)

temperature = (wl-iwl)/(wl*gf*(metal_constant+alpha)) + itp

strain = total_strain-temperature