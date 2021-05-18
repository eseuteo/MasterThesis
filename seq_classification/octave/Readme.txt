
HMMs reference and software library for Matlab/octave 
https://probml.github.io/pml-book/book0.html


The library pmtk3 can be found here:
https://code.google.com/archive/p/pmtk3/

# installation info:
https://code.google.com/archive/p/pmtk3/wikis/installation.wiki
https://github.com/probml/pmtk3




I installed (copied the pmtk3_master.zip from github) the pmtk3 in: /projects/sw/pmtk3-master/

For running the code in octave:

octave --no-gui

addpath('/projects/sw/pmtk3-master/')
run initPmtk3Octave
addpath('/projects/PLASS/SupplyChainMonitoring/octave/')
pkg load statistics   % for contingence table T=crosstab(X,Y)


