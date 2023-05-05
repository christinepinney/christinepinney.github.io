# cs436-final-project

* Link to repo: https://github.com/christinepinney/cs436-final-project

To run this code, run the cells to import necessary libraries. If any modules are not found, install the necessary packages. Any packages installed for this project are retained as comments in the `final_project` notebooks or in `packages.ipynb` (Note: some packages were installed but are not currently being used-- there were a couple iterations of this project where different libraries/packages were utilized). To work with the Sarcasm on Reddit dataset, run the cell to download the data directly from Kaggle and read from the generated csv. Theses files are very large, so they are not provided (they are automatically generated after running the download command below). For the News Headlines Dataset for Sarcasm Detection, the provided `Sarcasm_Headlines_Dataset.json` file contains the data, so parse this file to load the data into a dataframe. The Multinomial Naïve Bayes Classifier requires preprocessing using `CountVectorizer()`, and the provided `glasgow_stop_words.txt` file serves as the `stop_words` parameter. To reshape the data for ELECTRA, `LabelBinarizer()` is used to transform the provided labels into proper format. The tokenizer used is the ELECTRA Tokenizer.

Kaggle:
  ```
  od.download("https://www.kaggle.com/datasets/danofer/sarcasm?resource=download")

  data = pd.read_csv('sarcasm/train-balanced-sarcasm.csv')

  ```

News Headlines:
  ```
  def parse_data(file):
      for l in open(file,'r'):
          yield json.loads(l)

  data = list(parse_data('./Sarcasm_Headlines_Dataset.json'))

  df =  pd.read_json('Sarcasm_Headlines_Dataset.json', lines=True)
  df.rename(columns={'headline': 'text'}, inplace=True)
  ``` 

Preprocess for MNB Classifier:
  ```
  with open('glasgow_stop_words.txt') as f:
    stops = f.readlines()
  f.close()
  vec = CountVectorizer(stop_words=stops)
  bag_o_words = vec.fit_transform(df['text'])
  bag_o_words = np.array(bag_o_words.todense())
  ```
  
Preprocess for ELECTRA:
  ```
  training_data, testing_data = train_test_split(df, test_size = 0.08)

  training_data = training_data[['text', 'is_sarcastic']]
  testing_data = testing_data[['text', 'is_sarcastic']]

  train_y = pd.get_dummies(training_data.is_sarcastic)
  test_y = pd.get_dummies(testing_data.is_sarcastic)

  train_data = np.array(training_data['text'])
  test_data = np.array(testing_data['text'])

  train_labels = LabelBinarizer().fit_transform(train_y)
  test_labels = LabelBinarizer().fit_transform(test_y)


  tokenizer = ElectraTokenizer.from_pretrained('google/electra-small-discriminator')
  ```
