from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import CountVectorizer
import re
import joblib
import string


class Processor:
    def __init__(self) -> None:
        pass

    def to_lower(self, text):
        return text.lower()

    def clean(self, text):
        cleaned = re.compile(r'<.*?>')
        return re.sub(cleaned, '', text)

    def rem_stopwords(self, text):
        stop_words = set(stopwords.words('english'))
        words = word_tokenize(text)
        return "".join([w for w in words if w not in stop_words])
    
    def remove_punctuations(self, text):
        return text.translate(str.maketrans('', '', string.punctuation))

    def process(self, text):
        processed = self.remove_punctuations(self.rem_stopwords(self.clean(self.to_lower(text))))
        return [processed]