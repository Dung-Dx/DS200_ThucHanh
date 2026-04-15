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
    category;

base_reviews = DISTINCT base_reviews;

clean_reviews = FOREACH base_reviews GENERATE
    category,
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
    review_words::word AS word;

grouped_words = GROUP words_only BY (category, word);

word_count = FOREACH grouped_words GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(words_only) AS freq;

grouped_category = GROUP word_count BY category;

top5_words = FOREACH grouped_category GENERATE
    FLATTEN(TOP(5, 2, word_count));

STORE top5_words INTO '/Lab2/output/Bai5_top5_words_by_category' USING PigStorage('\t');