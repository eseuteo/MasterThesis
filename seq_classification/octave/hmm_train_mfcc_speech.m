function [yhat, ytest, model] = hmm_train_mfcc_speech()
%
%  run with:
%     addpath(pwd)
%     addpath('/projects/sw/pmtk3-master/')
%     run initPmtk3Octave
%     [yhat, ytest, model] = hmm_train_mfcc_speech();
%
%-----------------------

pkg load statistics   % for contingence table T=crosstab(X,Y)



% Read the train and test data
% example from: 
%   https://github.com/probml/pmtk3/blob/master/demos/isolatedWordClassificationWithHmmsDemo.m
loadData('speechDataDigits4And5'); 
nstates = 5;
setSeed(0); 
Xtrain = [train4'; train5'];
ytrain = [repmat(4, numel(train4), 1) ; repmat(5, numel(train5), 1)];
[Xtrain, ytrain] = shuffleRows(Xtrain, ytrain);
Xtest = test45'; 
ytest = labels'; 
[Xtest, ytest] = shuffleRows(Xtest, ytest); 


%-----------------------    
% Train the HMM models

%% Initial Guess
pi0 = [1, 0, 0, 0, 0]
%% Initial values used in the paper:

%% Initial transition matrix
transmat0 = normalize(diag(ones(nstates, 1)) + diag(ones(nstates-1, 1), 1), 2)

%%        
fitArgs = {'pi0', pi0, 'trans0', transmat0, 'maxIter', 10, 'verbose', true};
fitFn   = @(X)hmmFit(X, nstates, 'gauss', fitArgs{:}); 
model = generativeClassifierFit(fitFn, Xtrain, ytrain); 
%%

logprobFn = @hmmLogprob;
[yhat, post] = generativeClassifierPredict(logprobFn, model, Xtest);
fprintf("One Gaussian, confusion matrix and percentage\n")
%a = table(yhat, ytest)  %%% table is not available in octave
a = crosstab(yhat, ytest)
% Confusion matrix:
% a(1) a(3)
% a(2) a(4)

b1=a(1)+a(2);
b2=a(3)+a(4);
[a(1)/b1, a(3)/b2; a(2)/b1, a(4)/b2]*100
nerrors = sum(yhat ~= ytest);
percentage_error=100*(length(ytest)-nerrors)/length(ytest);
fprintf("Number of errors = %d,  percentage of accuracy= %f\n\n",nerrors, percentage_error)
 
%%
%%if 0
%% Do the same thing with a tied mixture of Gaussians observation model
nmix    = 3; 
fitArgs = [fitArgs, {'nmix', nmix}];
fitFn   = @(X)hmmFit(X, nstates, 'mixGaussTied', fitArgs{:}); 
model = generativeClassifierFit(fitFn, Xtrain, ytrain);

logprobFn = @hmmLogprob;
[yhat, post] = generativeClassifierPredict(logprobFn, model, Xtest);
%%
%%[yhat, ytest] 
fprintf("5 Gaussian mixture, confusion matrix and percentage\n")
%a = table(yhat, ytest)  %%% table is not available in octave
a = crosstab(yhat, ytest)
% Confusion matrix:
% a(1) a(3)
% a(2) a(4)

b1=a(1)+a(2);
b2=a(3)+a(4);
[a(1)/b1, a(3)/b2; a(2)/b1, a(4)/b2]*100
nerrors = sum(yhat ~= ytest);
display(nerrors);
percentage_error=100*(length(ytest)-nerrors)/length(ytest);
fprintf("Number of errors = %d,  percentage of accuracy= %f\n",nerrors, percentage_error)
