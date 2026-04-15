reviews = LOAD '/Lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

base_reviews = FOREACH reviews GENERATE
    id,
    review,
    category,
    sentiment;

base_reviews = DISTINCT base_reviews;

clean_reviews = FOREACH base_reviews GENERATE
    category,
    sentiment,
    TRIM(
        REPLACE(
            REPLACE(
                LOWER(review),
                '[^\\p{L}\\p{N}\\s]', ' '
            ),
            '\\s+', ' '
        )
    ) AS clean_review;

review_words = FOREACH clean_reviews GENERATE
    category,
    sentiment,
    FLATTEN(TOKENIZE(clean_review)) AS word;

review_words = FILTER review_words BY word IS NOT NULL AND word != '';

stopwords_raw = LOAD '/Lab2/stopwords.txt' USING PigStorage('\n') AS (stopword: chararray);

stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

joined_data = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;
filtered_words = FILTER joined_data BY stopwords::stopword IS NULL;

words_only = FOREACH filtered_words GENERATE
    review_words::category AS category,
    review_words::sentiment AS sentiment,
    review_words::word AS word;

grouped_words = GROUP words_only BY (category, sentiment, word);

word_count = FOREACH grouped_words GENERATE
    FLATTEN(group) AS (category, sentiment, word),
    COUNT(words_only) AS freq;

positive_words = FILTER word_count BY sentiment == 'positive';

grouped_positive = GROUP positive_words BY category;

top5_positive = FOREACH grouped_positive GENERATE
    FLATTEN(TOP(5, 3, positive_words));

negative_words = FILTER word_count BY sentiment == 'negative';

grouped_negative = GROUP negative_words BY category;

top5_negative = FOREACH grouped_negative GENERATE
    FLATTEN(TOP(5, 3, negative_words));

STORE top5_positive INTO '/Lab2/output/Bai4_top5_positive_words' USING PigStorage('\t');
STORE top5_negative INTO '/Lab2/output/Bai4_top5_negative_words' USING PigStorage('\t');