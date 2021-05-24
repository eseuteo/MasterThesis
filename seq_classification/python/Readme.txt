
Tutorial:
Sequence Classification with LSTM Recurrent Neural Networks in Python with Keras
https://machinelearningmastery.com/sequence-classification-lstm-recurrent-neural-networks-python-keras/


#--------------------
Results:

sequence sofa_score    train    BA   test     BA   Prec. Recall   
length   threshold
   3         4        419/113  0.91  131/95  0.47   0.3  0.04   (8/95)
   4         4        416/110  0.87  130/94  0.48   0.3  0.07   (7/94)
   5         4        412/108  0.91  129/93  0.49   0.4  0.06   (6/93)
   6         4        408/196  0.87  127/93  0.49   0.4  0.07   (7/93)
   7         4        406/102  0.91  127/91  0.51   0.4  0.07   (7/91)
-------------------------------------------------
model 1 units=600
   2         4        428/137  ~1    126/69  0.56   0.48 0.3    (21/69)
                                             0.55   0.45 0.3    (22/69)
   3         4        425/140  ~1    125/68  0.55   0.5  0.23   (16/68)
                                             0.57   0.55 0.27   (19/68)
   4         4        422/137   1    124/67  0.58   0.55 0.28   (19/67)
                                             0.55   0.5  0.22   (15/67)
   5         4        418/135   1    123/66  0.53   0.46 0.18   (12/66)					     



#-------------
python3 lstm_vitals_files.py 3 4
model 1: units=600
Train Sequences labels distribution:
0  1
1  0    425
0  1    140
dtype: int64

Confusion matrix:
[[425   0]
 [  1 139]]
 accuracy =  0.9982300884955753 
 balanced_accuracy =  0.9964285714285714 
 precision =  1.0 
 recall =  0.9928571428571429
13/13 [==============================] - 0s 3ms/step

Test Sequences labels distribution:
0  1
1  0    125
0  1     68
dtype: int64

Confusion matrix:
[[112  13]
 [ 53  15]]
 accuracy =  0.6580310880829016 
 balanced_accuracy =  0.5582941176470588 
 precision =  0.5357142857142857 
 recall =  0.22058823529411764


#------------------
python3 lstm_vitals_files.py 3 4
model2

Train Sequences labels distribution:
0  1
1  0    425
0  1    140
dtype: int64

Confusion matrix:
[[425   0]
 [  2 138]]
 accuracy =  0.9964601769911504 
 balanced_accuracy =  0.9928571428571429 
 precision =  1.0 
 recall =  0.9857142857142858
13/13 [==============================] - 0s 4ms/step

Test Sequences labels distribution:
0  1
1  0    125
0  1     68
dtype: int64

Confusion matrix:
[[115  10]
 [ 56  12]]
 accuracy =  0.6580310880829016 
 balanced_accuracy =  0.548235294117647 
 precision =  0.5454545454545454 
 recall =  0.17647058823529413






