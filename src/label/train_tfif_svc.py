# src/label/train_tfidf_svc.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report

def run(input_csv="data/outputs/labeled.csv"):
    df = pd.read_csv(input_csv)
    df = df.dropna(subset=["clean","label"])
    X_train, X_test, y_train, y_test = train_test_split(
        df["clean"], df["label"], test_size=0.2, random_state=42, stratify=df["label"]
    )
    pipe = Pipeline([
        ("tfidf", TfidfVectorizer(max_features=60000, ngram_range=(1,2))),
        ("clf", LinearSVC())
    ])
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    print(classification_report(y_test, y_pred, digits=3))

if __name__ == "__main__":
    run()
