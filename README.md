# Denormalization of ACS data

## Overview

This python script provides a clean and easy solution for ingesting
and denormalizing American Community Survey data for a state. The main
function takes state, year, and estimates precision as parameters and
generates datasets that contain all variables at census tract and block
group level in the specified location. FIPS codes are used as ids to
facilitate subsequent quering, spatial analysis, and joining with the
external data. 