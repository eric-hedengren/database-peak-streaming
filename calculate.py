gf = .807 # gage factor at 22 C 
wl = None # wavelength
iwl = None # initial wavelength
itp = 'measured' # initial temperature


total_strain = 10**6((wl-iwl)/iwl/gf)

temperature = (wl-iwl)/(wl*gf*(metal_constant+alpha)) + itp

strain = total_strain-temperature