# Run with:
#   python3 lstm_vitals.py sequence_length sofa_threshold
#   python3 lstm_vitals.py 3 4
#

# import libraries
import pandas as pd
import numpy as np
import sys
import tensorflow as tf
import os

# Setting seed for reproducibility
np.random.seed(1234)
PYTHONHASHSEED = 0

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    confusion_matrix,
    recall_score,
    precision_score,
    balanced_accuracy_score,
    accuracy_score,
)
from tensorflow.python.keras.models import Sequential, load_model


# function to generate sequences
def gen_sequence(id_df, seq_length, seq_cols):
    data_matrix = id_df[seq_cols].values
    # print(data_matrix)
    num_elements = data_matrix.shape[0]
    for start, stop in zip(
        range(0, num_elements - seq_length), range(seq_length, num_elements)
    ):
        print(start, stop)
        yield data_matrix[start:stop, :]


# function to generate labels
def gen_labels(id_df, seq_length, label):
    data_matrix = id_df[label].values
    # print(data_matrix)
    num_elements = data_matrix.shape[0]
    return data_matrix[seq_length:num_elements, :]


# Press the green button in the gutter to run the script.
if __name__ == "__main__":

    print("Number of arguments:", len(sys.argv), "arguments.")
    print("Argument List:", str(sys.argv))
    print("arg[1]", sys.argv[1], " arg[2]", sys.argv[2])

    sequence_length = int(sys.argv[1])
    sofa_threshold = int(sys.argv[2])

    # https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html
    scaler = StandardScaler()
    # pick a window size
    # sequence_length = 3
    path = "./matched_six_vs_cli_onset/Shock-Patient_id-89091-vs-cli.csv"
    # path = './matched_six_vs_cli_onset/Shock-patient_id-69272-vs-cli.csv'
    # path = './matched_six_vs_cli_onset/Non-shock-Patient_id-66152-vs-cli.csv'
    # path = '-/matched_six_vs_cli_onset/Non-shock-Patient_id-89734-vs-cli.csv'
    train_df = pd.read_csv(path)

    # pick the feature columns
    # sequence_cols = ['hr','abpSys','abpDias','abpMean','resp','sp02','SDhr','SDresp','SDsp02']
    sequence_cols = [
        "hr",
        "abpSys",
        "abpDias",
        "abpMean",
        "resp",
        "sp02",
        "SDhr",
        "SDabpSys",
        "SDabpDias",
        "SDabpMean",
        "SDresp",
        "SDsp02",
        "SEhr",
        "SEresp",
        "CorHRabpSys",
        "CorHRabpDias",
        "CorHRabpMean",
        "CorHRresp",
        "CorHRsp02",
        "CorRespSp02",
    ]
    sequence_cols_all = [
        "DateVitals",
        "hr",
        "abpSys",
        "abpDias",
        "abpMean",
        "resp",
        "sp02",
        "SDhr",
        "SDabpSys",
        "SDabpDias",
        "SDabpMean",
        "SDresp",
        "SDsp02",
        "SEhr",
        "SEresp",
        "CorHRabpSys",
        "CorHRabpDias",
        "CorHRabpMean",
        "CorHRresp",
        "CorHRsp02",
        "CorRespSp02",
        "sofa_Score",
        "mylabel",
    ]

    # first scale the values we are using as features
    train_df[sequence_cols] = scaler.fit_transform(train_df[sequence_cols])

    # here I am adding a label based on the sofa_Score
    # sofa_threshold = 5
    train_df["mylabel"] = np.where(
        train_df.sofa_Score > sofa_threshold, "shock", "Nonshock"
    )
    train_df = train_df.loc[:, sequence_cols_all]
    label_encoding = pd.get_dummies(train_df.mylabel)
    train_df = pd.concat([train_df, label_encoding], axis=1)

    # save train data to test with the Java LSTMTest
    train_df.to_csv("train_seq_data.csv")

    # generate the sequences, of size sequence_length
    seq_gen = list(gen_sequence(train_df, sequence_length, sequence_cols))
    seq_array = np.array(list(seq_gen)).astype(np.float32)
    print(seq_array.shape)

    # generate labels
    label_gen = gen_labels(train_df, sequence_length, ["Nonshock", "shock"])
    label_array = np.array(label_gen).astype(np.float32)
    # print(label_array)
    print(label_array.shape)

    # -----
    # Create model
    # number of features
    nb_features = seq_array.shape[2]
    # number of classes
    nb_out = label_array.shape[1]

    # create model
    # model = vi.create_bi_model(nb_features, nb_out)
    model = Sequential()
    model.add(
        tf.keras.layers.Bidirectional(
            tf.keras.layers.LSTM(units=64, input_shape=(nb_features, nb_out))
        )
    )
    model.add(tf.keras.layers.Dropout(rate=0.4))
    model.add(tf.keras.layers.Dense(units=nb_out))
    # opt = tf.keras.optimizers.SGD( 0.001)

    model.compile(loss="mean_squared_error", optimizer="Adam", metrics=["accuracy"])

    # fit the network
    history = model.fit(
        seq_array,
        label_array,
        epochs=5,
        batch_size=16,
        verbose=1,
        shuffle=False
        # ,validation_split=0.05,
        # callbacks = [tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta=0, patience=5, verbose=0, mode='min')]
        # ,tf.keras.callbacks.ModelCheckpoint(model_path,monitor='val_loss', save_best_only=True, mode='min', verbose=0)]
    )

    # list all data in history
    print(history.history.keys())

    scores = model.evaluate(seq_array, label_array, verbose=1, batch_size=16)
    print("\n----\nAccuracy: {}".format(scores[1]))

    # make predictions and compute confusion matrix
    y_pred = model.predict_classes(seq_array, verbose=1, batch_size=16)
    y_true = label_array
    print(y_pred)
    # print(y_true)

    y_true = np.argmax(y_true, axis=1)

    print(
        "Parameters: \nsequence length: ",
        sys.argv[1],
        "  sofa_score threshold: ",
        sys.argv[2],
    )

    print("\nLabels distribution (original data not the generated sequences):")
    print(train_df["mylabel"].value_counts())
    print("Sequences labels distribution:")
    print(pd.DataFrame(label_gen).value_counts())

    print("\nConfusion matrix:")
    cm = confusion_matrix(y_true, y_pred)
    print(cm)

    # compute precision and recall
    accuracy = accuracy_score(y_true, y_pred)
    balanced_accuracy = balanced_accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    print(
        " accuracy = ",
        accuracy,
        "\n balanced_accuracy = ",
        balanced_accuracy,
        "\n precision = ",
        precision,
        "\n",
        "recall = ",
        recall,
    )

    export_dir = "/projects/students/Master/MedicalSequences/FlinkSequences/seq_classification/python/tmp"

    # with tf.keras.backend.get_session() as sess:
    # with tf.compat.v1.keras.backend.get_session() as sess:
    #    tf.saved_model.simple_save(sess, export_dir, \
    #                   inputs= {"keys":model.input}, \
    #                    outputs= {t.name: t for t in model.outputs})

    # save the model
    model_name = "lstm_model_vitals"
    model_path = os.path.join(model_name)
    tf.saved_model.save(model, model_path)
