reviews = LOAD '/Lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

unique_reviews = FOREACH reviews GENERATE
    id,
    review;

unique_reviews = DISTINCT unique_reviews;

clean_reviews = FOREACH unique_reviews GENERATE
    id,
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
    FLATTEN(TOKENIZE(clean_review)) AS word;

review_words = FILTER review_words BY word IS NOT NULL AND word != '';

stopwords_raw = LOAD '/Lab2/stopwords.txt' USING PigStorage('\n') AS (stopword: chararray);

stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

joined_words = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;
filtered_words = FILTER joined_words BY stopwords::stopword IS NULL;

words_only = FOREACH filtered_words GENERATE
    review_words::word AS word;

grouped_words = GROUP words_only BY word;
word_frequency = FOREACH grouped_words GENERATE
    group AS word,
    COUNT(words_only) AS freq;

words_over_500 = FILTER word_frequency BY freq > 500;


grouped_category = GROUP reviews BY category;
category_count = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(reviews) AS total_comments;


grouped_aspect = GROUP reviews BY aspect;
aspect_count = FOREACH grouped_aspect GENERATE
    group AS aspect,
    COUNT(reviews) AS total_comments;

STORE words_over_500   INTO '/Lab2/output/Bai2_words_over_500' USING PigStorage('\t');
STORE category_count   INTO '/Lab2/output/Bai2_category_count' USING PigStorage('\t');
STORE aspect_count     INTO '/Lab2/output/Bai2_aspect_count' USING PigStorage('\t');