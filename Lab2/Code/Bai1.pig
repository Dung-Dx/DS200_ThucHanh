reviews = LOAD '/Lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

unique_reviews = DISTINCT reviews;

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
    ) AS clean_review,
    category,
    aspect,
    sentiment;

review_words = FOREACH clean_reviews GENERATE
    id,
    FLATTEN(TOKENIZE(clean_review)) AS word,
    category,
    aspect,
    sentiment;

review_words = FILTER review_words BY word IS NOT NULL AND word != '';

stopwords_raw = LOAD '/Lab2/stopwords.txt' USING PigStorage() AS (stopword: chararray);

stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

joined_data = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;

filtered_words = FILTER joined_data BY stopwords::stopword IS NULL;

result = FOREACH filtered_words GENERATE
    review_words::id        AS id,
    review_words::word      AS word,
    review_words::category  AS category,
    review_words::aspect    AS aspect,
    review_words::sentiment AS sentiment;

STORE result INTO '/Lab2/output/Bai1/' USING PigStorage(';');