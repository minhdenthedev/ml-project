import numpy as np
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB,MultinomialNB,BernoulliNB
from sklearn.metrics import accuracy_score, precision_score, recall_score
import joblib
# nltk.download() # Download "punkt" package
nltk.download('punkt')

## 1 | Data Preprocessing ##
"""Prepare the dataset before training"""

# 1.1 Load dataset
dataset = pd.read_csv('Dataset/IMDB.csv')

# 1.3 Encode output column into binary
dataset.sentiment.replace('positive', 1, inplace=True)
dataset.sentiment.replace('negative', 0, inplace=True)

## 2 | Data cleaning ##
"""Clean dataset reviews as following:
1. Remove HTML tags
2. Remove special characters
3. Convert everything to lowercase
4. Remove stopwords
5. Stemming
"""

# 2.1 Remove HTML tags
def clean(text):
    cleaned = re.compile(r'<.*?>')
    return re.sub(cleaned,'',text)

dataset.review = dataset.review.apply(clean)

# 2.2 Remove special characters
def is_special(text):
    rem = ''
    for i in text:
        if i.isalnum():
            rem = rem + i
        else:
            rem = rem + ' '
    return rem

dataset.review = dataset.review.apply(is_special)

# 2.3 Convert everything to lowercase
def to_lower(text):
    return text.lower()

dataset.review = dataset.review.apply(to_lower)

#2.4 Remove stopwords
def rem_stopwords(text):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(text)
    return [w for w in words if w not in stop_words]

dataset.review = dataset.review.apply(rem_stopwords)

# 2.5 Stem the words
def stem_txt(text):
    ss = SnowballStemmer('english')
    return " ".join([ss.stem(w) for w in text])

dataset.review = dataset.review.apply(stem_txt)

## 3 | Model Creation ##
"""Create model to fit it to the data"""

# 3.1 Creating Bag Of Words (BOW)
X = np.array(dataset.iloc[:,0].values)
y = np.array(dataset.sentiment.values)
cv = CountVectorizer(max_features = 2000)
X = cv.fit_transform(dataset.review).toarray()

# 3.2 Train test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=9)

# 3.3 Defining the models and Training them
gnb = joblib.load('Models/MRSA_gnb.pkl')
mnb = joblib.load('Models/MRSA_mnb.pkl')
bnb = joblib.load('Models/MRSA_bnb.pkl')

# 3.5 Make predictions
ypg = gnb.predict(X_test)
ypm = mnb.predict(X_test)
ypb = bnb.predict(X_test)

## 4 | Model Evaluation ##
"""Evaluate model performance"""

print(f"Gaussian accuracy    =  {round(accuracy_score(y_test, ypg), 2)*100} %")
print(f'Gaussian precision   =  {round(precision_score(y_test, ypg), 2)*100}%')
print(f'Gaussian recall   =  {round(recall_score(y_test, ypg), 2)*100}%\n')

print(f"Multinomial accuracy =  {round(accuracy_score(y_test, ypm), 2)*100} %")
print(f"Multinomial precision =  {round(precision_score(y_test, ypm), 2)*100} %")
print(f"Multinomial recall =  {round(recall_score(y_test, ypm), 2)*100} %\n")

print(f"Bernoulli accuracy   =  {round(accuracy_score(y_test, ypb), 2)*100} %")
print(f"Bernoulli precision   =  {round(precision_score(y_test, ypb), 2)*100} %")
print(f"Bernoulli recall   =  {round(recall_score(y_test, ypb), 2)*100} %")

